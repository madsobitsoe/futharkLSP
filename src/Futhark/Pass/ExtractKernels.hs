{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
-- | Kernel extraction.
--
-- In the following, I will use the term "width" to denote the amount
-- of immediate parallelism in a map - that is, the outer size of the
-- array(s) being used as input.
--
-- = Basic Idea
--
-- If we have:
--
-- @
--   map
--     map(f)
--     bnds_a...
--     map(g)
-- @
--
-- Then we want to distribute to:
--
-- @
--   map
--     map(f)
--   map
--     bnds_a
--   map
--     map(g)
-- @
--
-- But for now only if
--
--  (0) it can be done without creating irregular arrays.
--      Specifically, the size of the arrays created by @map(f)@, by
--      @map(g)@ and whatever is created by @bnds_a@ that is also used
--      in @map(g)@, must be invariant to the outermost loop.
--
--  (1) the maps are _balanced_.  That is, the functions @f@ and @g@
--      must do the same amount of work for every iteration.
--
-- The advantage is that the map-nests containing @map(f)@ and
-- @map(g)@ can now be trivially flattened at no cost, thus exposing
-- more parallelism.  Note that the @bnds_a@ map constitutes array
-- expansion, which requires additional storage.
--
-- = Distributing Sequential Loops
--
-- As a starting point, sequential loops are treated like scalar
-- expressions.  That is, not distributed.  However, sometimes it can
-- be worthwhile to distribute if they contain a map:
--
-- @
--   map
--     loop
--       map
--     map
-- @
--
-- If we distribute the loop and interchange the outer map into the
-- loop, we get this:
--
-- @
--   loop
--     map
--       map
--   map
--     map
-- @
--
-- Now more parallelism may be available.
--
-- = Unbalanced Maps
--
-- Unbalanced maps will as a rule be sequentialised, but sometimes,
-- there is another way.  Assume we find this:
--
-- @
--   map
--     map(f)
--       map(g)
--     map
-- @
--
-- Presume that @map(f)@ is unbalanced.  By the simple rule above, we
-- would then fully sequentialise it, resulting in this:
--
-- @
--   map
--     loop
--   map
--     map
-- @
--
-- == Balancing by Loop Interchange
--
-- The above is not ideal, as we cannot flatten the @map-loop@ nest,
-- and we are thus limited in the amount of parallelism available.
--
-- But assume now that the width of @map(g)@ is invariant to the outer
-- loop.  Then if possible, we can interchange @map(f)@ and @map(g)@,
-- sequentialise @map(f)@ and distribute, interchanging the outer
-- parallel loop into the sequential loop:
--
-- @
--   loop(f)
--     map
--       map(g)
--   map
--     map
-- @
--
-- After flattening the two nests we can obtain more parallelism.
--
-- When distributing a map, we also need to distribute everything that
-- the map depends on - possibly as its own map.  When distributing a
-- set of scalar bindings, we will need to know which of the binding
-- results are used afterwards.  Hence, we will need to compute usage
-- information.
--
-- = Redomap
--
-- Redomap can be handled much like map.  Distributed loops are
-- distributed as maps, with the parameters corresponding to the
-- neutral elements added to their bodies.  The remaining loop will
-- remain a redomap.  Example:
--
-- @
-- redomap(op,
--         fn (v) =>
--           map(f)
--           map(g),
--         e,a)
-- @
--
-- distributes to
--
-- @
-- let b = map(fn v =>
--               let acc = e
--               map(f),
--               a)
-- redomap(op,
--         fn (v,dist) =>
--           map(g),
--         e,a,b)
-- @
--
-- Note that there may be further kernel extraction opportunities
-- inside the @map(f)@.  The downside of this approach is that the
-- intermediate array (@b@ above) must be written to main memory.  An
-- often better approach is to just turn the entire @redomap@ into a
-- single kernel.
--
module Futhark.Pass.ExtractKernels
       (extractKernels)
       where

import Control.Monad.RWS.Strict
import Control.Monad.Reader
import Control.Monad.Writer.Strict
import Control.Monad.Trans.Maybe
import qualified Data.Set as S
import Data.Maybe
import Data.List

import Futhark.Representation.SOACS
import qualified Futhark.Representation.SOACS.SOAC as SOAC
import Futhark.Representation.SOACS.Simplify (simplifyStms, simpleSOACS)
import qualified Futhark.Representation.Kernels as Out
import Futhark.Representation.Kernels.Kernel
import Futhark.MonadFreshNames
import Futhark.Tools
import qualified Futhark.Transform.FirstOrderTransform as FOT
import qualified Futhark.Pass.ExtractKernels.Kernelise as Kernelise
import Futhark.Transform.Rename
import Futhark.Pass
import Futhark.Transform.CopyPropagate
import Futhark.Pass.ExtractKernels.Distribution
import Futhark.Pass.ExtractKernels.ISRWIM
import Futhark.Pass.ExtractKernels.BlockedKernel
import Futhark.Pass.ExtractKernels.Interchange
import Futhark.Pass.ExtractKernels.Intragroup
import Futhark.Util
import Futhark.Util.Log

type KernelsStms = Out.Stms Out.Kernels
type InKernelStms = Out.Stms Out.InKernel
type InKernelLambda = Out.Lambda Out.InKernel

-- | Transform a program using SOACs to a program using explicit
-- kernels, using the kernel extraction transformation.
extractKernels :: Pass SOACS Out.Kernels
extractKernels =
  Pass { passName = "extract kernels"
       , passDescription = "Perform kernel extraction"
       , passFunction = fmap Prog . mapM transformFunDef . progFunctions
       }

-- In order to generate more stable threshold names, we keep track of
-- the numbers used for thresholds separately from the ordinary name
-- source,
data State = State { stateNameSource :: VNameSource
                   , stateThresholdCounter :: Int
                   }

newtype DistribM a = DistribM (RWS (Scope Out.Kernels) Log State a)
                   deriving (Functor, Applicative, Monad,
                             HasScope Out.Kernels, LocalScope Out.Kernels,
                             MonadState State,
                             MonadLogger)

instance MonadFreshNames DistribM where
  getNameSource = gets stateNameSource
  putNameSource src = modify $ \s -> s { stateNameSource = src }

runDistribM :: (MonadLogger m, MonadFreshNames m) =>
               DistribM a -> m a
runDistribM (DistribM m) = do
  (x, msgs) <- modifyNameSource $ \src ->
    let (x, s, msgs) = runRWS m mempty (State src 0)
    in ((x, msgs), stateNameSource s)
  addLog msgs
  return x

transformFunDef :: (MonadFreshNames m, MonadLogger m) =>
                   FunDef -> m (Out.FunDef Out.Kernels)
transformFunDef (FunDef entry name rettype params body) = runDistribM $ do
  body' <- localScope (scopeOfFParams params) $
           transformBody mempty body
  return $ FunDef entry name rettype params body'

transformBody :: KernelPath -> Body -> DistribM (Out.Body Out.Kernels)
transformBody path body = do bnds <- transformStms path $ stmsToList $ bodyStms body
                             return $ mkBody bnds $ bodyResult body

transformStms :: KernelPath -> [Stm] -> DistribM KernelsStms
transformStms _ [] =
  return mempty
transformStms path (bnd:bnds) =
  sequentialisedUnbalancedStm bnd >>= \case
    Nothing -> do
      bnd' <- transformStm path bnd
      inScopeOf bnd' $
        (bnd'<>) <$> transformStms path bnds
    Just bnds' ->
      transformStms path $ stmsToList bnds' <> bnds

sequentialisedUnbalancedStm :: Stm -> DistribM (Maybe (Stms SOACS))
sequentialisedUnbalancedStm (Let pat _ (Op soac@(Screma _ form _)))
  | Just (_, lam2) <- isRedomapSOAC form,
    unbalancedLambda lam2, lambdaContainsParallelism lam2 = do
      types <- asksScope scopeForSOACs
      Just . snd <$> runBinderT (FOT.transformSOAC pat soac) types
sequentialisedUnbalancedStm _ =
  return Nothing

scopeForSOACs :: Scope Out.Kernels -> Scope SOACS
scopeForSOACs = castScope

scopeForKernels :: Scope SOACS -> Scope Out.Kernels
scopeForKernels = castScope

transformStm :: KernelPath -> Stm -> DistribM KernelsStms

transformStm path (Let pat aux (Op (CmpThreshold what s))) = do
  ((r, _), stms) <- cmpSizeLe s (Out.SizeThreshold path) what
  runBinder_ $ do
    addStms stms
    addStm $ Let pat aux $ BasicOp $ SubExp r

transformStm path (Let pat aux (If c tb fb rt)) = do
  tb' <- transformBody path tb
  fb' <- transformBody path fb
  return $ oneStm $ Let pat aux $ If c tb' fb' rt

transformStm path (Let pat aux (DoLoop ctx val form body)) =
  localScope (castScope (scopeOf form) <>
              scopeOfFParams mergeparams) $
    oneStm . Let pat aux . DoLoop ctx val form' <$> transformBody path body
  where mergeparams = map fst $ ctx ++ val
        form' = case form of
                  WhileLoop cond ->
                    WhileLoop cond
                  ForLoop i it bound ps ->
                    ForLoop i it bound ps

transformStm path (Let pat (StmAux cs _) (Op (Screma w form arrs)))
  | Just lam <- isMapSOAC form =
      distributeMap path $ MapLoop pat cs w lam arrs

transformStm path (Let res_pat (StmAux cs _) (Op (Screma w form arrs)))
  | Just (scan_lam, nes) <- isScanSOAC form,
    Just do_iswim <- iswim res_pat w scan_lam $ zip nes arrs = do
      types <- asksScope scopeForSOACs
      transformStms path =<< (stmsToList . snd <$> runBinderT (certifying cs do_iswim) types)

  -- We are only willing to generate code for scanomaps that do not
  -- involve array accumulators, and do not have parallelism in their
  -- map function.  Such cases will fall through to the
  -- screma-splitting case, and produce an ordinary map and scan.
  -- Hopefully, the scan then triggers the ISWIM case above (otherwise
  -- we will still crash in code generation).
  | Just (scan_lam, nes, map_lam) <- isScanomapSOAC form,
    all primType $ lambdaReturnType scan_lam,
    not $ lambdaContainsParallelism map_lam = do
      scan_lam_sequential <- Kernelise.transformLambda scan_lam
      map_lam_sequential <- Kernelise.transformLambda map_lam
      segScan res_pat w w scan_lam_sequential map_lam_sequential nes arrs [] []

transformStm path (Let res_pat (StmAux cs _) (Op (Screma w form arrs)))
  | Just [Reduce comm red_fun nes] <- isReduceSOAC form,
    let comm' | commutativeLambda red_fun = Commutative
              | otherwise                 = comm,
    Just do_irwim <- irwim res_pat w comm' red_fun $ zip nes arrs = do
      types <- asksScope scopeForSOACs
      bnds <- fst <$> runBinderT (simplifyStms =<< collectStms_ (certifying cs do_irwim)) types
      transformStms path $ stmsToList bnds

transformStm path (Let pat (StmAux cs _) (Op (Screma w form arrs)))
  | Just (reds, map_lam) <- isRedomapSOAC form = do

  let paralleliseOuter = runBinder_ $ do
        red_ops <- forM reds $ \(Reduce comm red_lam nes) -> do
          (red_lam', nes', shape) <- determineReduceOp red_lam nes
          return $ SegRedOp comm red_lam' nes' shape
        map_lam_sequential <- Kernelise.transformLambda map_lam
        addStms =<<
          (fmap (certify cs) <$> nonSegRed pat w red_ops map_lam_sequential arrs)

      outerParallelBody =
        renameBody =<<
        (mkBody <$> paralleliseOuter <*> pure (map Var (patternNames pat)))

      paralleliseInner path' = do
        (mapbnd, redbnd) <- redomapToMapAndReduce pat (w, comm', red_lam, map_lam, nes, arrs)
        transformStms path' [certify cs mapbnd, certify cs redbnd]
          where comm' | commutativeLambda red_lam = Commutative
                      | otherwise = comm
                (Reduce comm red_lam nes) = singleReduce reds

      innerParallelBody path' =
        renameBody =<<
        (mkBody <$> paralleliseInner path' <*> pure (map Var (patternNames pat)))

  if not $ lambdaContainsParallelism map_lam
    then paralleliseOuter
    else if incrementalFlattening then do
    ((outer_suff, outer_suff_key), suff_stms) <-
      sufficientParallelism "suff_outer_redomap" w path

    outer_stms <- outerParallelBody
    inner_stms <- innerParallelBody ((outer_suff_key, False):path)

    (suff_stms<>) <$> kernelAlternatives pat inner_stms [(outer_suff, outer_stms)]
    else paralleliseOuter

-- Streams can be handled in two different ways - either we
-- sequentialise the body or we keep it parallel and distribute.
transformStm path (Let pat (StmAux cs _) (Op (Stream w (Parallel _ _ _ []) map_fun arrs))) = do
  -- No reduction part.  Remove the stream and leave the body
  -- parallel.  It will be distributed.
  types <- asksScope scopeForSOACs
  transformStms path =<<
    (stmsToList . snd <$> runBinderT (certifying cs $ sequentialStreamWholeArray pat w [] map_fun arrs) types)

transformStm path (Let pat aux@(StmAux cs _) (Op (Stream w (Parallel o comm red_fun nes) fold_fun arrs)))
  | incrementalFlattening = do
      ((outer_suff, outer_suff_key), suff_stms) <-
        sufficientParallelism "suff_outer_stream" w path

      outer_stms <- outerParallelBody ((outer_suff_key, True) : path)
      inner_stms <- innerParallelBody ((outer_suff_key, False) : path)

      (suff_stms<>) <$> kernelAlternatives pat inner_stms [(outer_suff, outer_stms)]

  | otherwise = paralleliseOuter path

  where
    paralleliseOuter path'
      | any (not . primType) $ lambdaReturnType red_fun = do
          -- Split into a chunked map and a reduction, with the latter
          -- further transformed.
          fold_fun_sequential <- Kernelise.transformLambda fold_fun

          let (red_pat_elems, concat_pat_elems) =
                splitAt (length nes) $ patternValueElements pat
              red_pat = Pattern [] red_pat_elems

          ((num_threads, red_results), stms) <-
            streamMap (map (baseString . patElemName) red_pat_elems) concat_pat_elems w
            Noncommutative fold_fun_sequential nes arrs

          reduce_soac <- reduceSOAC [Reduce comm' red_fun nes]

          (stms<>) <$>
            inScopeOf stms
            (transformStm path' $ Let red_pat aux $
             Op (Screma num_threads reduce_soac red_results))

      | otherwise = do
          red_fun_sequential <- Kernelise.transformLambda red_fun
          fold_fun_sequential <- Kernelise.transformLambda fold_fun
          fmap (certify cs) <$>
            streamRed pat w comm' red_fun_sequential fold_fun_sequential nes arrs

    outerParallelBody path' =
      renameBody =<<
      (mkBody <$> paralleliseOuter path' <*> pure (map Var (patternNames pat)))

    paralleliseInner path' = do
      types <- asksScope scopeForSOACs
      transformStms path' . fmap (certify cs) =<<
        (stmsToList . snd <$> runBinderT (sequentialStreamWholeArray pat w nes fold_fun arrs) types)

    innerParallelBody path' =
      renameBody =<<
      (mkBody <$> paralleliseInner path' <*> pure (map Var (patternNames pat)))

    comm' | commutativeLambda red_fun, o /= InOrder = Commutative
          | otherwise                               = comm

transformStm path (Let pat (StmAux cs _) (Op (Screma w form arrs))) = do
  -- This screma is too complicated for us to immediately do
  -- anything, so split it up and try again.
  scope <- asksScope scopeForSOACs
  transformStms path . map (certify cs) . stmsToList . snd =<<
    runBinderT (dissectScrema pat w form arrs) scope

transformStm path (Let pat _ (Op (Stream w (Sequential nes) fold_fun arrs))) = do
  -- Remove the stream and leave the body parallel.  It will be
  -- distributed.
  types <- asksScope scopeForSOACs
  transformStms path =<<
    (stmsToList . snd <$> runBinderT (sequentialStreamWholeArray pat w nes fold_fun arrs) types)

transformStm _ (Let pat (StmAux cs _) (Op (Scatter w lam ivs as))) = runBinder_ $ do
  lam' <- Kernelise.transformLambda lam
  write_i <- newVName "write_i"
  let (as_ws, as_ns, as_vs) = unzip3 as
      (i_res, v_res) = splitAt (sum as_ns) $ bodyResult $ lambdaBody lam'
      kstms = bodyStms $ lambdaBody lam'
      krets = do (a_w, a, is_vs) <- zip3 as_ws as_vs $ chunks as_ns $ zip i_res v_res
                 return $ WriteReturn [a_w] a [ ([i],v) | (i,v) <- is_vs ]
      body = KernelBody () kstms krets
      inputs = do (p, p_a) <- zip (lambdaParams lam') ivs
                  return $ KernelInput (paramName p) (paramType p) p_a [Var write_i]
  (bnds, kernel) <-
    mapKernel w (FlatThreadSpace [(write_i,w)]) inputs (map rowType $ patternTypes pat) body
  certifying cs $ do
    addStms bnds
    letBind_ pat $ Op $ HostOp kernel

transformStm _ (Let orig_pat (StmAux cs _) (Op (GenReduce w ops bucket_fun imgs))) = do
  bfun' <- Kernelise.transformLambda bucket_fun
  genReduceKernel orig_pat [] [] cs w ops bfun' imgs

transformStm _ bnd =
  runBinder_ $ FOT.transformStmRecursively bnd

data MapLoop = MapLoop Pattern Certificates SubExp Lambda [VName]

mapLoopStm :: MapLoop -> Stm
mapLoopStm (MapLoop pat cs w lam arrs) = Let pat (StmAux cs ()) $ Op $ Screma w (mapSOAC lam) arrs

sufficientParallelism :: String -> SubExp -> KernelPath
                      -> DistribM ((SubExp, Name), Out.Stms Out.Kernels)
sufficientParallelism desc what path = cmpSizeLe desc (Out.SizeThreshold path) what

distributeMap :: KernelPath -> MapLoop -> DistribM KernelsStms
distributeMap path (MapLoop pat cs w lam arrs) = do
  types <- askScope
  let loopnest = MapNesting pat cs w $ zip (lambdaParams lam) arrs
      env path' = KernelEnv { kernelNest =
                                singleNesting (Nesting mempty loopnest)
                            , kernelScope =
                                scopeForKernels (scopeOf lam) <> types
                            , kernelPath =
                                path'
                            }
      exploitInnerParallelism path' = do
        (acc', postkernels) <- runKernelM (env path') $
          distribute =<< distributeMapBodyStms acc (bodyStms $ lambdaBody lam)

        -- There may be a few final targets remaining - these correspond to
        -- arrays that are identity mapped, and must have statements
        -- inserted here.
        return $ postKernelsStms postkernels <>
          identityStms (outerTarget $ kernelTargets acc')

  if not incrementalFlattening then exploitInnerParallelism path
    else do

    let exploitOuterParallelism path' = do
          soactypes <- asksScope scopeForSOACs
          (seq_lam, _) <- runBinderT (Kernelise.transformLambda lam) soactypes
          (acc', postkernels) <- runKernelM (env path') $ distribute $
            addStmsToKernel (bodyStms $ lambdaBody seq_lam) acc
          -- As above, we deal with identity mappings.
          return $ postKernelsStms postkernels <>
            identityStms (outerTarget $ kernelTargets acc')

    distributeMap' id (newKernel loopnest) path exploitOuterParallelism exploitInnerParallelism pat w lam
    where acc = KernelAcc { kernelTargets = singleTarget (pat, bodyResult $ lambdaBody lam)
                          , kernelStms = mempty
                          }

          params_to_arrs = zip (map paramName $ lambdaParams lam) arrs
          identityStms (rem_pat, res) =
            stmsFromList $ zipWith identityStm (patternValueElements rem_pat) res
          identityStm pe (Var v)
            | Just arr <- lookup v params_to_arrs =
                Let (Pattern [] [pe]) (defAux ()) $ BasicOp $ Copy arr
          identityStm pe se =
            Let (Pattern [] [pe]) (defAux ()) $ BasicOp $ Replicate (Shape [w]) se

distributeMap' :: (HasScope Out.Kernels m, MonadFreshNames m) =>
                  (forall a. DistribM a -> m a)
               -> KernelNest -> KernelPath
               -> (KernelPath -> m (Out.Stms Out.Kernels))
               -> (KernelPath -> m (Out.Stms Out.Kernels))
               -> PatternT Type
               -> SubExp
               -> LambdaT SOACS
               -> m (Out.Stms Out.Kernels)
distributeMap' distribM loopnest path mk_seq_stms mk_par_stms pat nest_w lam = do
  let res = map Var $ patternNames pat

  types <- askScope
  ((outer_suff, outer_suff_key), outer_suff_stms) <-
    distribM $ sufficientParallelism "suff_outer_par" nest_w path

  intra <- if worthIntraGroup lam then
             flip runReaderT types $ intraGroupParallelise loopnest lam
           else return Nothing
  seq_body <- renameBody =<< mkBody <$>
              mk_seq_stms ((outer_suff_key, True) : path) <*> pure res
  let seq_alts = [(outer_suff, seq_body) | worthSequentialising lam]

  case intra of
    Nothing -> do
      par_body <- renameBody =<< mkBody <$>
                  mk_par_stms ((outer_suff_key, False) : path) <*> pure res

      (outer_suff_stms<>) <$> kernelAlternatives pat par_body seq_alts

    Just ((_intra_min_par, intra_avail_par), group_size, intra_prelude, intra_stms) -> do
      -- We must check that all intra-group parallelism fits in a group.
      ((intra_ok, intra_suff_key), intra_suff_stms) <- do

        ((intra_suff, suff_key), check_suff_stms) <-
          distribM $ sufficientParallelism "suff_intra_par" intra_avail_par $
          (outer_suff_key, False) : path

        runBinder $ do

          addStms intra_prelude

          max_group_size <-
            letSubExp "max_group_size" $ Op $ Out.GetSizeMax Out.SizeGroup
          fits <- letSubExp "fits" $ BasicOp $
                  CmpOp (CmpSle Int32) group_size max_group_size

          addStms check_suff_stms

          intra_ok <- letSubExp "intra_suff_and_fits" $ BasicOp $ BinOp LogAnd fits intra_suff
          return (intra_ok, suff_key)

      group_par_body <- renameBody $ mkBody intra_stms res

      par_body <- renameBody =<< mkBody <$>
                  mk_par_stms ([(outer_suff_key, False),
                                (intra_suff_key, False)]
                                ++ path) <*> pure res

      ((outer_suff_stms<>intra_suff_stms)<>) <$>
        kernelAlternatives pat par_body (seq_alts ++ [(intra_ok, group_par_body)])

data KernelEnv = KernelEnv { kernelNest :: Nestings
                           , kernelScope :: Scope Out.Kernels
                           , kernelPath :: KernelPath
                           }

data KernelAcc = KernelAcc { kernelTargets :: Targets
                           , kernelStms :: InKernelStms
                           }

data KernelRes = KernelRes { accPostKernels :: PostKernels
                           , accLog :: Log
                           }

instance Semigroup KernelRes where
  KernelRes ks1 log1 <> KernelRes ks2 log2 =
    KernelRes (ks1 <> ks2) (log1 <> log2)

instance Monoid KernelRes where
  mempty = KernelRes mempty mempty

newtype PostKernel = PostKernel { unPostKernel :: KernelsStms }

newtype PostKernels = PostKernels [PostKernel]

instance Semigroup PostKernels where
  PostKernels xs <> PostKernels ys = PostKernels $ ys ++ xs

instance Monoid PostKernels where
  mempty = PostKernels mempty

postKernelsStms :: PostKernels -> KernelsStms
postKernelsStms (PostKernels kernels) = mconcat $ map unPostKernel kernels

typeEnvFromKernelAcc :: KernelAcc -> Scope Out.Kernels
typeEnvFromKernelAcc = scopeOfPattern . fst . outerTarget . kernelTargets

addStmsToKernel :: InKernelStms -> KernelAcc -> KernelAcc
addStmsToKernel stms acc =
  acc { kernelStms = stms <> kernelStms acc }

addStmToKernel :: (LocalScope Out.Kernels m, MonadFreshNames m) =>
                  Stm -> KernelAcc -> m KernelAcc
addStmToKernel bnd acc = do
  stms <- runBinder_ $ Kernelise.transformStm bnd
  return acc { kernelStms = stms <> kernelStms acc }

newtype KernelM a = KernelM (ReaderT KernelEnv (WriterT KernelRes DistribM) a)
  deriving (Functor, Applicative, Monad,
            MonadReader KernelEnv,
            MonadWriter KernelRes)

liftDistribM :: DistribM a -> KernelM a
liftDistribM m = do
  scope <- askScope
  KernelM $ lift $ lift $ localScope scope m

instance MonadFreshNames KernelM where
  getNameSource = KernelM $ lift getNameSource
  putNameSource = KernelM . lift . putNameSource

instance HasScope Out.Kernels KernelM where
  askScope = asks kernelScope

instance LocalScope Out.Kernels KernelM where
  localScope types = local $ \env ->
    env { kernelScope = types <> kernelScope env }

instance MonadLogger KernelM where
  addLog msgs = tell mempty { accLog = msgs }

runKernelM :: KernelEnv -> KernelM a -> DistribM (a, PostKernels)
runKernelM env (KernelM m) = do
  (x, res) <- runWriterT $ runReaderT m env
  addLog $ accLog res
  return (x, accPostKernels res)

collectKernels :: KernelM a -> KernelM (a, PostKernels)
collectKernels m = pass $ do
  (x, res) <- listen m
  return ((x, accPostKernels res),
          const res { accPostKernels = mempty })

collectKernels_ :: KernelM () -> KernelM PostKernels
collectKernels_ = fmap snd . collectKernels

localPath :: KernelPath -> KernelM a -> KernelM a
localPath path = local $ \env -> env { kernelPath = path }

addKernels :: PostKernels -> KernelM ()
addKernels ks = tell $ mempty { accPostKernels = ks }

addKernel :: KernelsStms -> KernelM ()
addKernel bnds = addKernels $ PostKernels [PostKernel bnds]

withStm :: Stm -> KernelM a -> KernelM a
withStm bnd = local $ \env ->
  env { kernelScope =
          scopeForKernels (scopeOf [bnd]) <> kernelScope env
      , kernelNest =
          letBindInInnerNesting provided $
          kernelNest env
      }
  where provided = S.fromList $ patternNames $ stmPattern bnd

mapNesting :: Pattern -> Certificates -> SubExp -> Lambda -> [VName]
           -> KernelM a
           -> KernelM a
mapNesting pat cs w lam arrs = local $ \env ->
  env { kernelNest = pushInnerNesting nest $ kernelNest env
      , kernelScope =  scopeForKernels (scopeOf lam) <> kernelScope env
      }
  where nest = Nesting mempty $
               MapNesting pat cs w $
               zip (lambdaParams lam) arrs

inNesting :: KernelNest -> KernelM a -> KernelM a
inNesting (outer, nests) = local $ \env ->
  env { kernelNest = (inner, nests')
      , kernelScope =  mconcat (map scopeOf $ outer : nests) <> kernelScope env
      }
  where (inner, nests') =
          case reverse nests of
            []           -> (asNesting outer, [])
            (inner' : ns) -> (asNesting inner', map asNesting $ outer : reverse ns)
        asNesting = Nesting mempty

unbalancedLambda :: Lambda -> Bool
unbalancedLambda lam =
  unbalancedBody
  (S.fromList $ map paramName $ lambdaParams lam) $
  lambdaBody lam

  where subExpBound (Var i) bound = i `S.member` bound
        subExpBound (Constant _) _ = False

        unbalancedBody bound body =
          any (unbalancedStm (bound <> boundInBody body) . stmExp) $
          bodyStms body

        -- XXX - our notion of balancing is probably still too naive.
        unbalancedStm bound (Op (Stream w _ _ _)) =
          w `subExpBound` bound
        unbalancedStm bound (Op (Screma w _ _)) =
          w `subExpBound` bound
        unbalancedStm _ Op{} =
          False
        unbalancedStm _ DoLoop{} = False

        unbalancedStm bound (If cond tbranch fbranch _) =
          cond `subExpBound` bound &&
          (unbalancedBody bound tbranch || unbalancedBody bound fbranch)

        unbalancedStm _ (BasicOp _) =
          False
        unbalancedStm _ (Apply fname _ _ _) =
          not $ isBuiltInFunction fname

bodyContainsParallelism :: Body -> Bool
bodyContainsParallelism = any (isMap . stmExp) . bodyStms
  where isMap Op{} = True
        isMap _ = False

lambdaContainsParallelism :: Lambda -> Bool
lambdaContainsParallelism = bodyContainsParallelism . lambdaBody

-- | Returns the sizes of nested parallelism.
nestedParallelism :: Body -> [SubExp]
nestedParallelism = concatMap (parallelism . stmExp) . bodyStms
  where parallelism (Op (Scatter w _ _ _)) = [w]
        parallelism (Op (Screma w _ _)) = [w]
        parallelism (Op (Stream w Sequential{} lam _))
          | chunk_size_param : _ <- lambdaParams lam =
              let update (Var v) | v == paramName chunk_size_param = w
                  update se = se
              in map update $ nestedParallelism $ lambdaBody lam
        parallelism (DoLoop _ _ _ body) = nestedParallelism body
        parallelism _ = []

-- | A lambda is worth sequentialising if it contains nested
-- parallelism of an interesting kind.
worthSequentialising :: Lambda -> Bool
worthSequentialising lam = interesting $ lambdaBody lam
  where interesting body = any (interesting' . stmExp) $ bodyStms body
        interesting' (Op (Screma _ form@(ScremaForm _ _ lam') _))
          | isJust $ isMapSOAC form = worthSequentialising lam'
        interesting' (Op Scatter{}) = False -- Basically a map.
        interesting' (DoLoop _ _ _ body) = interesting body
        interesting' (Op _) = True
        interesting' _ = False

-- | Intra-group parallelism is worthwhile if the lambda contains
-- non-map nested parallelism, or any nested parallelism inside a
-- loop.
worthIntraGroup :: Lambda -> Bool
worthIntraGroup lam = interesting $ lambdaBody lam
  where interesting body = not (null $ nestedParallelism body) &&
                           not (onlyMaps $ bodyStms body)
        onlyMaps = all $ isMapOrSeq . stmExp
        isMapOrSeq (Op (Screma _ form@(ScremaForm _ _ lam') _))
          | isJust $ isMapSOAC form = not $ worthIntraGroup lam'
        isMapOrSeq (Op Scatter{}) = True -- Basically a map.
        isMapOrSeq (DoLoop _ _ _ body) =
          null $ nestedParallelism body
        isMapOrSeq (Op _) = False
        isMapOrSeq _ = True

-- Enable if you want the cool new versioned code.  Beware: may be
-- slower in practice.  Caveat emptor (and you are the emptor).
incrementalFlattening :: Bool
incrementalFlattening = isJust $ lookup "FUTHARK_INCREMENTAL_FLATTENING" unixEnvironment

distributeInnerMap :: MapLoop -> KernelAcc
                   -> KernelM KernelAcc
distributeInnerMap maploop@(MapLoop pat cs w lam arrs) acc
  | unbalancedLambda lam, lambdaContainsParallelism lam =
      addStmToKernel (mapLoopStm maploop) acc
  | not incrementalFlattening =
      distributeNormally
  | otherwise =
      distributeSingleStm acc (mapLoopStm maploop) >>= \case
      Just (post_kernels, res, nest, acc')
        | Just (perm, _pat_unused) <- permutationAndMissing pat res -> do
            addKernels post_kernels
            multiVersion perm nest acc'
      _ -> distributeNormally
  where
    lam_bnds = bodyStms $ lambdaBody lam
    lam_res = bodyResult $ lambdaBody lam

    def_acc = KernelAcc { kernelTargets = pushInnerTarget
                          (pat, bodyResult $ lambdaBody lam) $
                          kernelTargets acc
                        , kernelStms = mempty
                        }

    distributeNormally =
      distribute =<<
      leavingNesting maploop =<<
      mapNesting pat cs w lam arrs
      (distribute =<< distributeMapBodyStms def_acc lam_bnds)

    multiVersion perm nest acc' = do
      -- The kernel can be distributed by itself, so now we can
      -- decide whether to just sequentialise, or exploit inner
      -- parallelism.
      let map_nesting = MapNesting pat cs w $ zip (lambdaParams lam) arrs
          -- Normally the permutation is for the output pattern, but
          -- we can't really change that, so we change the result
          -- order instead.
          lam_res' = rearrangeShape (rearrangeInverse perm) lam_res
          nest' = pushInnerKernelNesting (pat, lam_res') map_nesting nest
          extra_scope = targetsScope $ kernelTargets acc'

          exploitInnerParallelism path' =
            fmap postKernelsStms $ collectKernels_ $ localPath path' $
            localScope extra_scope $ inNesting nest' $ void $
            distribute =<< leavingNesting maploop =<< distribute =<<
            distributeMapBodyStms def_acc lam_bnds

      -- XXX: we do not construct a new KernelPath when
      -- sequentialising.  This is only OK as long as further
      -- versioning does not take place down that branch (it currently
      -- does not).
      (nestw_bnds, nestw, sequentialised_kernel) <- localScope extra_scope $ do
        sequentialised_map_body <-
          localScope (scopeOfLParams (lambdaParams lam)) $ runBinder_ $
          Kernelise.transformStms lam_bnds
        let kbody = KernelBody () sequentialised_map_body $
                    map ThreadsReturn lam_res'
        constructKernel nest' kbody

      let outer_pat = loopNestingPattern $ fst nest
      path <- asks kernelPath
      addKernel =<< (nestw_bnds<>) <$>
        localScope extra_scope (distributeMap' liftDistribM nest' path
                                (const $ return $ oneStm sequentialised_kernel)
                                exploitInnerParallelism
                                outer_pat nestw
                                lam { lambdaBody = (lambdaBody lam) { bodyResult = lam_res' }})

      return acc'

leavingNesting :: MapLoop -> KernelAcc -> KernelM KernelAcc
leavingNesting (MapLoop _ cs w lam arrs) acc =
  case popInnerTarget $ kernelTargets acc of
   Nothing ->
     fail "The kernel targets list is unexpectedly small"
   Just ((pat,res), newtargets) -> do
     let acc' = acc { kernelTargets = newtargets }
     if null $ kernelStms acc'
       then return acc'
       else do let kbody = Body () (kernelStms acc') res
                   used_in_body = freeIn kbody
                   (used_params, used_arrs) =
                     unzip $
                     filter ((`S.member` used_in_body) . paramName . fst) $
                     zip (lambdaParams lam) arrs
               stms <- runBinder_ $ Kernelise.mapIsh pat cs w used_params kbody used_arrs
               return $ addStmsToKernel stms acc' { kernelStms = mempty }

distributeMapBodyStms :: KernelAcc -> Stms SOACS -> KernelM KernelAcc
distributeMapBodyStms orig_acc = onStms orig_acc . stmsToList
  where
    onStms acc [] = return acc

    onStms acc (Let pat (StmAux cs _) (Op (Stream w (Sequential accs) lam arrs)):stms) = do
      types <- asksScope scopeForSOACs
      stream_stms <-
        snd <$> runBinderT (sequentialStreamWholeArray pat w accs lam arrs) types
      stream_stms' <-
        runReaderT (copyPropagateInStms simpleSOACS stream_stms) types
      onStms acc $ stmsToList (fmap (certify cs) stream_stms') ++ stms

    onStms acc (stm:stms) =
      -- It is important that stm is in scope if 'maybeDistributeStm'
      -- wants to distribute, even if this causes the slightly silly
      -- situation that stm is in scope of itself.
      withStm stm $ maybeDistributeStm stm =<< onStms acc stms

maybeDistributeStm :: Stm -> KernelAcc -> KernelM KernelAcc

maybeDistributeStm bnd@(Let pat _ (Op (Screma w form arrs))) acc
  | Just lam <- isMapSOAC form =
  -- Only distribute inside the map if we can distribute everything
  -- following the map.
  distributeIfPossible acc >>= \case
    Nothing -> addStmToKernel bnd acc
    Just acc' -> distribute =<< distributeInnerMap (MapLoop pat (stmCerts bnd) w lam arrs) acc'

maybeDistributeStm bnd@(Let pat _ (DoLoop [] val form@ForLoop{} body)) acc
  | null (patternContextElements pat), bodyContainsParallelism body =
  distributeSingleStm acc bnd >>= \case
    Just (kernels, res, nest, acc')
      | S.null $ freeIn form `S.intersection` boundInKernelNest nest,
        Just (perm, pat_unused) <- permutationAndMissing pat res ->
          -- We need to pretend pat_unused was used anyway, by adding
          -- it to the kernel nest.
          localScope (typeEnvFromKernelAcc acc') $ do
          addKernels kernels
          nest' <- expandKernelNest pat_unused nest
          types <- asksScope scopeForSOACs

          bnds <- runReaderT
                  (interchangeLoops nest' (SeqLoop perm pat val form body)) types
          path <- asks kernelPath
          bnds' <- liftDistribM $ transformStms path $ stmsToList bnds
          addKernel bnds'
          return acc'
    _ ->
      addStmToKernel bnd acc

maybeDistributeStm stm@(Let pat _ (If cond tbranch fbranch ret)) acc
  | null (patternContextElements pat),
    bodyContainsParallelism tbranch || bodyContainsParallelism fbranch ||
    any (not . primType) (ifReturns ret) =
    distributeSingleStm acc stm >>= \case
      Just (kernels, res, nest, acc')
        | S.null $ (freeIn cond <> freeIn ret) `S.intersection`
          boundInKernelNest nest,
          Just (perm, pat_unused) <- permutationAndMissing pat res ->
            -- We need to pretend pat_unused was used anyway, by adding
            -- it to the kernel nest.
            localScope (typeEnvFromKernelAcc acc') $ do
            nest' <- expandKernelNest pat_unused nest
            addKernels kernels
            types <- asksScope scopeForSOACs
            let branch = Branch perm pat cond tbranch fbranch ret
            stms <- runReaderT (interchangeBranch nest' branch) types

            path <- asks kernelPath
            stms' <- liftDistribM $ transformStms path $ stmsToList stms
            addKernel stms'
            return acc'
      _ ->
        addStmToKernel stm acc

maybeDistributeStm (Let pat (StmAux cs _) (Op (Screma w form arrs))) acc
  | Just [Reduce comm lam nes] <- isReduceSOAC form,
    Just m <- irwim pat w comm lam $ zip nes arrs = do
      types <- asksScope scopeForSOACs
      (_, bnds) <- runBinderT (certifying cs m) types
      distributeMapBodyStms acc bnds

-- Parallelise segmented scatters.
maybeDistributeStm bnd@(Let pat (StmAux cs _) (Op (Scatter w lam ivs as))) acc =
  distributeSingleStm acc bnd >>= \case
    Just (kernels, res, nest, acc')
      | Just (perm, pat_unused) <- permutationAndMissing pat res ->
        localScope (typeEnvFromKernelAcc acc') $ do
          nest' <- expandKernelNest pat_unused nest
          lam' <- Kernelise.transformLambda lam
          addKernels kernels
          addKernel =<< segmentedScatterKernel nest' perm pat cs w lam' ivs as
          return acc'
    _ ->
      addStmToKernel bnd acc

-- Parallelise segmented GenReduce.
maybeDistributeStm bnd@(Let pat (StmAux cs _) (Op (GenReduce w ops lam as))) acc =
  distributeSingleStm acc bnd >>= \case
    Just (kernels, res, nest, acc')
      | Just (perm, pat_unused) <- permutationAndMissing pat res ->
        localScope (typeEnvFromKernelAcc acc') $ do
          lam' <- Kernelise.transformLambda lam
          nest' <- expandKernelNest pat_unused nest
          addKernels kernels
          addKernel =<< segmentedGenReduceKernel nest' perm cs w ops lam' as
          return acc'
    _ ->
      addStmToKernel bnd acc

-- If the scan can be distributed by itself, we will turn it into a
-- segmented scan.
--
-- If the scan cannot be distributed by itself, it will be
-- sequentialised in the default case for this function.
maybeDistributeStm bnd@(Let pat (StmAux cs _) (Op (Screma w form arrs))) acc
  | Just (lam, nes, map_lam) <- isScanomapSOAC form =
  distributeSingleStm acc bnd >>= \case
    Just (kernels, res, nest, acc')
      | Just (perm, pat_unused) <- permutationAndMissing pat res ->
          -- We need to pretend pat_unused was used anyway, by adding
          -- it to the kernel nest.
          localScope (typeEnvFromKernelAcc acc') $ do
          nest' <- expandKernelNest pat_unused nest
          map_lam' <- Kernelise.transformLambda map_lam
          lam' <- Kernelise.transformLambda lam
          localScope (typeEnvFromKernelAcc acc') $
            segmentedScanomapKernel nest' perm w lam' map_lam' nes arrs >>=
            kernelOrNot cs bnd acc kernels acc'
    _ ->
      addStmToKernel bnd acc

-- If the reduction can be distributed by itself, we will turn it into a
-- segmented reduce.
--
-- If the reduction cannot be distributed by itself, it will be
-- sequentialised in the default case for this function.
maybeDistributeStm bnd@(Let pat (StmAux cs _) (Op (Screma w form arrs))) acc
  | Just (reds, map_lam) <- isRedomapSOAC form,
    Reduce comm lam nes <- singleReduce reds,
    isIdentityLambda map_lam || incrementalFlattening =
  distributeSingleStm acc bnd >>= \case
    Just (kernels, res, nest, acc')
      | Just (perm, pat_unused) <- permutationAndMissing pat res ->
          -- We need to pretend pat_unused was used anyway, by adding
          -- it to the kernel nest.
          localScope (typeEnvFromKernelAcc acc') $ do
          nest' <- expandKernelNest pat_unused nest
          lam' <- Kernelise.transformLambda lam
          map_lam' <- Kernelise.transformLambda map_lam

          let comm' | commutativeLambda lam = Commutative
                    | otherwise             = comm

          regularSegmentedRedomapKernel nest' perm w comm' lam' map_lam' nes arrs >>=
            kernelOrNot cs bnd acc kernels acc'
    _ ->
      addStmToKernel bnd acc

maybeDistributeStm (Let pat (StmAux cs _) (Op (Screma w form arrs))) acc
  | incrementalFlattening || isNothing (isRedomapSOAC form) = do
  -- This with-loop is too complicated for us to immediately do
  -- anything, so split it up and try again.
  scope <- asksScope scopeForSOACs
  distributeMapBodyStms acc . fmap (certify cs) . snd =<<
    runBinderT (dissectScrema pat w form arrs) scope

maybeDistributeStm (Let pat aux (BasicOp (Replicate (Shape (d:ds)) v))) acc
  | [t] <- patternTypes pat = do
      -- XXX: We need a temporary dummy binding to prevent an empty
      -- map body.  The kernel extractor does not like empty map
      -- bodies.
      tmp <- newVName "tmp"
      let rowt = rowType t
          newbnd = Let pat aux $ Op $ Screma d (mapSOAC lam) []
          tmpbnd = Let (Pattern [] [PatElem tmp rowt]) aux $
                   BasicOp $ Replicate (Shape ds) v
          lam = Lambda { lambdaReturnType = [rowt]
                       , lambdaParams = []
                       , lambdaBody = mkBody (oneStm tmpbnd) [Var tmp]
                       }
      maybeDistributeStm newbnd acc

maybeDistributeStm bnd@(Let _ aux (BasicOp Copy{})) acc =
  distributeSingleUnaryStm acc bnd $ \_ outerpat arr ->
  return $ oneStm $ Let outerpat aux $ BasicOp $ Copy arr

-- Opaques are applied to the full array, because otherwise they can
-- drastically inhibit parallelisation in some cases.
maybeDistributeStm bnd@(Let (Pattern [] [pe]) aux (BasicOp Opaque{})) acc
  | not $ primType $ typeOf pe =
      distributeSingleUnaryStm acc bnd $ \_ outerpat arr ->
      return $ oneStm $ Let outerpat aux $ BasicOp $ Copy arr

maybeDistributeStm bnd@(Let _ aux (BasicOp (Rearrange perm _))) acc =
  distributeSingleUnaryStm acc bnd $ \nest outerpat arr -> do
    let r = length (snd nest) + 1
        perm' = [0..r-1] ++ map (+r) perm
    -- We need to add a copy, because the original map nest
    -- will have produced an array without aliases, and so must we.
    arr' <- newVName $ baseString arr
    arr_t <- lookupType arr
    return $ stmsFromList
      [Let (Pattern [] [PatElem arr' arr_t]) aux $ BasicOp $ Copy arr,
       Let outerpat aux $ BasicOp $ Rearrange perm' arr']

maybeDistributeStm bnd@(Let _ aux (BasicOp (Reshape reshape _))) acc =
  distributeSingleUnaryStm acc bnd $ \nest outerpat arr -> do
    let reshape' = map DimNew (kernelNestWidths nest) ++
                   map DimNew (newDims reshape)
    return $ oneStm $ Let outerpat aux $ BasicOp $ Reshape reshape' arr

maybeDistributeStm stm@(Let _ aux (BasicOp (Rotate rots _))) acc =
  distributeSingleUnaryStm acc stm $ \nest outerpat arr -> do
    let rots' = map (const $ intConst Int32 0) (kernelNestWidths nest) ++ rots
    return $ oneStm $ Let outerpat aux $ BasicOp $ Rotate rots' arr

-- XXX?  This rule is present to avoid the case where an in-place
-- update is distributed as its own kernel, as this would mean thread
-- then writes the entire array that it updated.  This is problematic
-- because the in-place updates is O(1), but writing the array is
-- O(n).  It is OK if the in-place update is preceded, followed, or
-- nested inside a sequential loop or similar, because that will
-- probably be O(n) by itself.  As a hack, we only distribute if there
-- does not appear to be a loop following.  The better solution is to
-- depend on memory block merging for this optimisation, but it is not
-- ready yet.
maybeDistributeStm (Let pat aux (BasicOp (Update arr [DimFix i] v))) acc
  | [t] <- patternTypes pat,
    arrayRank t == 1,
    not $ any (amortises . stmExp) $ kernelStms acc = do
      let w = arraySize 0 t
          et = stripArray 1 t
          lam = Lambda { lambdaParams = []
                       , lambdaReturnType = [Prim int32, et]
                       , lambdaBody = mkBody mempty [i, v] }
      maybeDistributeStm (Let pat aux $ Op $ Scatter (intConst Int32 1) lam [] [(w, 1, arr)]) acc
  where amortises DoLoop{} = True
        amortises Op{} = True
        amortises _ = False

maybeDistributeStm stm@(Let _ aux (BasicOp (Concat d x xs w))) acc =
  distributeSingleStm acc stm >>= \case
    Just (kernels, _, nest, acc') ->
      localScope (typeEnvFromKernelAcc acc') $
      segmentedConcat nest >>=
      kernelOrNot (stmAuxCerts aux) stm acc kernels acc'
    _ ->
      addStmToKernel stm acc

  where segmentedConcat nest =
          isSegmentedOp nest [0] w mempty mempty [] (x:xs) $
          \pat _ _ _ _ (x':xs') _ ->
            let d' = d + length (snd nest) + 1
            in addStm $ Let pat aux $ BasicOp $ Concat d' x' xs' w

maybeDistributeStm bnd acc =
  addStmToKernel bnd acc

distributeSingleUnaryStm :: KernelAcc
                             -> Stm
                             -> (KernelNest -> Pattern -> VName -> KernelM (Stms Out.Kernels))
                             -> KernelM KernelAcc
distributeSingleUnaryStm acc bnd f =
  distributeSingleStm acc bnd >>= \case
    Just (kernels, res, nest, acc')
      | res == map Var (patternNames $ stmPattern bnd),
        (outer, inners) <- nest,
        [(arr_p, arr)] <- loopNestingParamsAndArrs outer,
        boundInKernelNest nest `S.intersection` freeIn bnd
        == S.singleton (paramName arr_p) -> do
          addKernels kernels
          let outerpat = loopNestingPattern $ fst nest
          localScope (typeEnvFromKernelAcc acc') $ do
            (arr', pre_stms) <- repeatMissing arr (outer:inners)
            f_stms <- inScopeOf pre_stms $ f nest outerpat arr'
            addKernel $ pre_stms <> f_stms
            return acc'
    _ -> addStmToKernel bnd acc
  where -- | For an imperfectly mapped array, repeat the missing
        -- dimensions to make it look like it was in fact perfectly
        -- mapped.
        repeatMissing arr inners = do
          arr_t <- lookupType arr
          let shapes = determineRepeats arr arr_t inners
          if all (==Shape []) shapes then return (arr, mempty)
            else do
            let (outer_shapes, inner_shape) = repeatShapes shapes arr_t
                arr_t' = repeatDims outer_shapes inner_shape arr_t
            arr' <- newVName $ baseString arr
            return (arr', oneStm $ Let (Pattern [] [PatElem arr' arr_t']) (defAux ()) $
                          BasicOp $ Repeat outer_shapes inner_shape arr)

        determineRepeats arr arr_t nests
          | (skipped, arr_nest:nests') <- break (hasInput arr) nests,
            [(arr_p, _)] <- loopNestingParamsAndArrs arr_nest =
              Shape (map loopNestingWidth skipped) :
              determineRepeats (paramName arr_p) (rowType arr_t) nests'
          | otherwise =
              Shape (map loopNestingWidth nests) : replicate (arrayRank arr_t) (Shape [])

        hasInput arr nest
          | [(_, arr')] <- loopNestingParamsAndArrs nest, arr' == arr = True
          | otherwise = False


distribute :: KernelAcc -> KernelM KernelAcc
distribute acc =
  fromMaybe acc <$> distributeIfPossible acc

distributeIfPossible :: KernelAcc -> KernelM (Maybe KernelAcc)
distributeIfPossible acc = do
  nest <- asks kernelNest
  tryDistribute nest (kernelTargets acc) (kernelStms acc) >>= \case
    Nothing -> return Nothing
    Just (targets, kernel) -> do
      addKernel kernel
      return $ Just KernelAcc { kernelTargets = targets
                              , kernelStms = mempty
                              }

distributeSingleStm :: KernelAcc -> Stm
                        -> KernelM (Maybe (PostKernels, Result, KernelNest, KernelAcc))
distributeSingleStm acc bnd = do
  nest <- asks kernelNest
  tryDistribute nest (kernelTargets acc) (kernelStms acc) >>= \case
    Nothing -> return Nothing
    Just (targets, distributed_bnds) ->
      tryDistributeStm nest targets bnd >>= \case
        Nothing -> return Nothing
        Just (res, targets', new_kernel_nest) ->
          return $ Just (PostKernels [PostKernel distributed_bnds],
                         res,
                         new_kernel_nest,
                         KernelAcc { kernelTargets = targets'
                                   , kernelStms = mempty
                                   })

segmentedScatterKernel :: KernelNest
                       -> [Int]
                       -> Pattern
                       -> Certificates
                       -> SubExp
                       -> InKernelLambda
                       -> [VName] -> [(SubExp,Int,VName)]
                       -> KernelM KernelsStms
segmentedScatterKernel nest perm scatter_pat cs scatter_w lam ivs dests = do
  -- We replicate some of the checking done by 'isSegmentedOp', but
  -- things are different because a scatter is not a reduction or
  -- scan.
  --
  -- First, pretend that the scatter is also part of the nesting.  The
  -- KernelNest we produce here is technically not sensible, but it's
  -- good enough for flatKernel to work.
  let nest' = pushInnerKernelNesting (scatter_pat, bodyResult $ lambdaBody lam)
              (MapNesting scatter_pat cs scatter_w $ zip (lambdaParams lam) ivs) nest
  (nest_bnds, w, ispace, kernel_inps) <- flatKernel nest'

  let (as_ws, as_ns, as) = unzip3 dests

  -- The input/output arrays ('as') _must_ correspond to some kernel
  -- input, or else the original nested scatter would have been
  -- ill-typed.  Find them.
  as_inps <- mapM (findInput kernel_inps) as

  runBinder_ $ do
    addStms nest_bnds

    let rts = concatMap (take 1) $ chunks as_ns $
              drop (sum as_ns) $ lambdaReturnType lam
        (is,vs) = splitAt (sum as_ns) $ bodyResult $ lambdaBody lam
        k_body = KernelBody () (bodyStms $ lambdaBody lam) $
                 map (inPlaceReturn ispace) $
                 zip3 as_ws as_inps $ chunks as_ns $ zip is vs

    (k_bnds, k) <-
      mapKernel w (FlatThreadSpace ispace) kernel_inps rts k_body

    addStms k_bnds

    let pat = Pattern [] $ rearrangeShape perm $
              patternValueElements $ loopNestingPattern $ fst nest

    certifying cs $ letBind_ pat $ Op $ HostOp k
  where findInput kernel_inps a =
          maybe bad return $ find ((==a) . kernelInputName) kernel_inps
        bad = fail "Ill-typed nested scatter encountered."

        inPlaceReturn ispace (aw, inp, is_vs) =
          WriteReturn (init ws++[aw]) (kernelInputArray inp)
          [ (map Var (init gtids)++[i], v) | (i,v) <- is_vs ]
          where (gtids,ws) = unzip ispace

segmentedGenReduceKernel :: KernelNest
                         -> [Int]
                         -> Certificates
                         -> SubExp
                         -> [SOAC.GenReduceOp SOACS]
                         -> InKernelLambda
                         -> [VName]
                         -> KernelM KernelsStms
segmentedGenReduceKernel nest perm cs genred_w ops lam arrs = do
  -- We replicate some of the checking done by 'isSegmentedOp', but
  -- things are different because a GenReduce is not a reduction or
  -- scan.
  (nest_stms, _, ispace, inputs) <- flatKernel nest
  let orig_pat = Pattern [] $ rearrangeShape perm $
                 patternValueElements $ loopNestingPattern $ fst nest

  -- The input/output arrays _must_ correspond to some kernel input,
  -- or else the original nested GenReduce would have been ill-typed.
  -- Find them.
  ops' <- forM ops $ \(SOAC.GenReduceOp num_bins dests nes op) ->
    SOAC.GenReduceOp num_bins
    <$> mapM (fmap kernelInputArray . findInput inputs) dests
    <*> pure nes
    <*> pure op
  liftDistribM $ (nest_stms<>) <$>
    inScopeOf nest_stms
    (genReduceKernel orig_pat ispace inputs cs genred_w ops' lam arrs)
  where findInput kernel_inps a =
          maybe bad return $ find ((==a) . kernelInputName) kernel_inps
        bad = fail "Ill-typed nested GenReduce encountered."

genReduceKernel :: Pattern -> [(VName, SubExp)] -> [KernelInput]
                -> Certificates -> SubExp -> [SOAC.GenReduceOp SOACS]
                -> InKernelLambda -> [VName]
                -> DistribM KernelsStms
genReduceKernel orig_pat ispace inputs cs genred_w ops lam arrs = runBinder_ $ do
  ops' <- forM ops $ \(SOAC.GenReduceOp num_bins dests nes op) -> do
    (op', nes', shape) <- determineReduceOp op nes
    return $ Out.GenReduceOp num_bins dests nes' shape op'

  let isDest = flip elem $ concatMap Out.genReduceDest ops'
      inputs' = filter (not . isDest . kernelInputArray) inputs

  certifying cs $
    addStms =<< segGenRed orig_pat genred_w ispace inputs' ops' lam arrs

determineReduceOp :: (MonadBinder m, Lore m ~ Out.Kernels) =>
                     Lambda -> [SubExp] -> m (Out.Lambda Out.InKernel, [SubExp], Shape)
determineReduceOp lam nes =
  -- FIXME? We are assuming that the accumulator is a replicate, and
  -- we fish out its value in a gross way.
  case mapM subExpVar nes of
    Just ne_vs' -> do
      let (shape, lam') = isVectorMap lam
      nes' <- forM ne_vs' $ \ne_v -> do
        ne_v_t <- lookupType ne_v
        letSubExp "genred_ne" $
          BasicOp $ Index ne_v $ fullSlice ne_v_t $
          replicate (shapeRank shape) $ DimFix $ intConst Int32 0
      lam'' <- Kernelise.transformLambda lam'
      return (lam'', nes', shape)
    Nothing -> do
      lam' <- Kernelise.transformLambda lam
      return (lam', nes, mempty)

isVectorMap :: Lambda -> (Shape, Lambda)
isVectorMap lam
  | [Let (Pattern [] pes) _ (Op (Screma w form arrs))] <-
      stmsToList $ bodyStms $ lambdaBody lam,
    bodyResult (lambdaBody lam) == map (Var . patElemName) pes,
    Just map_lam <- isMapSOAC form,
    arrs == map paramName (lambdaParams lam) =
      let (shape, lam') = isVectorMap map_lam
      in (Shape [w] <> shape, lam')
  | otherwise = (mempty, lam)

segmentedScanomapKernel :: KernelNest
                        -> [Int]
                        -> SubExp
                        -> InKernelLambda -> InKernelLambda
                        -> [SubExp] -> [VName]
                        -> KernelM (Maybe KernelsStms)
segmentedScanomapKernel nest perm segment_size lam map_lam nes arrs =
  isSegmentedOp nest perm segment_size (freeIn lam) (freeIn map_lam) nes arrs $
  \pat total_num_elements ispace inps nes' _ _ ->
    addStms =<< segScan pat total_num_elements segment_size lam map_lam nes' arrs ispace inps

regularSegmentedRedomapKernel :: KernelNest
                              -> [Int]
                              -> SubExp -> Commutativity
                              -> InKernelLambda -> InKernelLambda -> [SubExp] -> [VName]
                              -> KernelM (Maybe KernelsStms)
regularSegmentedRedomapKernel nest perm segment_size comm lam map_lam nes arrs =
  isSegmentedOp nest perm segment_size (freeIn lam) (freeIn map_lam) nes arrs $
    \pat total_num_elements ispace inps nes' _ _ -> do
      let red_op = SegRedOp comm lam nes' mempty
      addStms =<< segRed pat total_num_elements segment_size [red_op] map_lam arrs ispace inps

isSegmentedOp :: KernelNest
              -> [Int]
              -> SubExp
              -> Names -> Names
              -> [SubExp] -> [VName]
              -> (Pattern
                  -> SubExp
                  -> [(VName, SubExp)]
                  -> [KernelInput]
                  -> [SubExp] -> [VName]  -> [VName]
                  -> Binder Out.Kernels ())
              -> KernelM (Maybe KernelsStms)
isSegmentedOp nest perm segment_size free_in_op _free_in_fold_op nes arrs m = runMaybeT $ do
  -- We must verify that array inputs to the operation are inputs to
  -- the outermost loop nesting or free in the loop nest.  Nothing
  -- free in the op may be bound by the nest.  Furthermore, the
  -- neutral elements must be free in the loop nest.
  --
  -- We must summarise any names from free_in_op that are bound in the
  -- nest, and describe how to obtain them given segment indices.

  let bound_by_nest = boundInKernelNest nest

  (pre_bnds, nesting_size, ispace, kernel_inps) <- flatKernel nest

  unless (S.null $ free_in_op `S.intersection` bound_by_nest) $
    fail "Non-fold lambda uses nest-bound parameters."

  let indices = map fst ispace

      prepareNe (Var v) | v `S.member` bound_by_nest =
                          fail "Neutral element bound in nest"
      prepareNe ne = return ne

      prepareArr arr =
        case find ((==arr) . kernelInputName) kernel_inps of
          Just inp
            | kernelInputIndices inp == map Var indices ->
                return $ return $ kernelInputArray inp
            | not (kernelInputArray inp `S.member` bound_by_nest) ->
                return $ replicateMissing ispace inp
          Nothing | not (arr `S.member` bound_by_nest) ->
                      -- This input is something that is free inside
                      -- the loop nesting. We will have to replicate
                      -- it.
                      return $
                      letExp (baseString arr ++ "_repd")
                      (BasicOp $ Replicate (Shape [nesting_size]) $ Var arr)
          _ ->
            fail "Input not free or outermost."

  nes' <- mapM prepareNe nes

  mk_arrs <- mapM prepareArr arrs

  lift $ runBinder_ $ do
    addStms pre_bnds

    -- We must make sure all inputs are of size
    -- segment_size*nesting_size.
    total_num_elements <-
      letSubExp "total_num_elements" $ BasicOp $ BinOp (Mul Int32) segment_size nesting_size

    let flatten arr = do
          arr_shape <- arrayShape <$> lookupType arr
          -- CHECKME: is the length the right thing here?  We want to
          -- reproduce the parameter type.
          let reshape = reshapeOuter [DimNew total_num_elements]
                        (2+length (snd nest)) arr_shape
          letExp (baseString arr ++ "_flat") $
            BasicOp $ Reshape reshape arr

    nested_arrs <- sequence mk_arrs
    arrs' <- mapM flatten nested_arrs

    let pat = Pattern [] $ rearrangeShape perm $
              patternValueElements $ loopNestingPattern $ fst nest

    m pat total_num_elements ispace kernel_inps nes' nested_arrs arrs'

  where replicateMissing ispace inp = do
          t <- lookupType $ kernelInputArray inp
          let inp_is = kernelInputIndices inp
              shapes = determineRepeats ispace inp_is
              (outer_shapes, inner_shape) = repeatShapes shapes t
          letExp "repeated" $ BasicOp $
            Repeat outer_shapes inner_shape $ kernelInputArray inp

        determineRepeats ispace (i:is)
          | (skipped_ispace, ispace') <- span ((/=i) . Var . fst) ispace =
              Shape (map snd skipped_ispace) : determineRepeats (drop 1 ispace') is
        determineRepeats ispace _ =
          [Shape $ map snd ispace]

permutationAndMissing :: Pattern -> [SubExp] -> Maybe ([Int], [PatElem])
permutationAndMissing pat res = do
  let pes = patternValueElements pat
      (_used,unused) =
        partition ((`S.member` freeIn res) . patElemName) pes
      res_expanded = res ++ map (Var . patElemName) unused
  perm <- map (Var . patElemName) pes `isPermutationOf` res_expanded
  return (perm, unused)

-- Add extra pattern elements to every kernel nesting level.
expandKernelNest :: MonadFreshNames m =>
                    [PatElem] -> KernelNest -> m KernelNest
expandKernelNest pes (outer_nest, inner_nests) = do
  let outer_size = loopNestingWidth outer_nest :
                   map loopNestingWidth inner_nests
      inner_sizes = tails $ map loopNestingWidth inner_nests
  outer_nest' <- expandWith outer_nest outer_size
  inner_nests' <- zipWithM expandWith inner_nests inner_sizes
  return (outer_nest', inner_nests')
  where expandWith nest dims = do
           pes' <- mapM (expandPatElemWith dims) pes
           return nest { loopNestingPattern =
                           Pattern [] $
                           patternElements (loopNestingPattern nest) <> pes'
                       }

        expandPatElemWith dims pe = do
          name <- newVName $ baseString $ patElemName pe
          return pe { patElemName = name
                    , patElemAttr = patElemType pe `arrayOfShape` Shape dims
                    }

cmpSizeLe :: String -> Out.SizeClass -> SubExp
          -> DistribM ((SubExp, Name), Out.Stms Out.Kernels)
cmpSizeLe desc size_class to_what = do
  x <- gets stateThresholdCounter
  modify $ \s -> s { stateThresholdCounter = x + 1}
  let size_key = nameFromString $ desc ++ "_" ++ show x
  runBinder $ do
    cmp_res <- letSubExp desc $ Op $ CmpSizeLe size_key size_class to_what
    return (cmp_res, size_key)

kernelAlternatives :: (MonadFreshNames m, HasScope Out.Kernels m) =>
                      Out.Pattern Out.Kernels
                   -> Out.Body Out.Kernels
                   -> [(SubExp, Out.Body Out.Kernels)]
                   -> m (Out.Stms Out.Kernels)
kernelAlternatives pat default_body [] = runBinder_ $ do
  ses <- bodyBind default_body
  forM_ (zip (patternNames pat) ses) $ \(name, se) ->
    letBindNames_ [name] $ BasicOp $ SubExp se
kernelAlternatives pat default_body ((cond,alt):alts) = runBinder_ $ do
  alts_pat <- fmap (Pattern []) $ forM (patternElements pat) $ \pe -> do
    name <- newVName $ baseString $ patElemName pe
    return pe { patElemName = name }

  alt_stms <- kernelAlternatives alts_pat default_body alts
  let alt_body = mkBody alt_stms $ map Var $ patternValueNames alts_pat

  letBind_ pat $ If cond alt alt_body $ ifCommon $ patternTypes pat

kernelOrNot :: Certificates -> Stm -> KernelAcc
            -> PostKernels -> KernelAcc -> Maybe KernelsStms
            -> KernelM KernelAcc
kernelOrNot cs bnd acc _ _ Nothing =
  addStmToKernel (certify cs bnd) acc
kernelOrNot cs _ _ kernels acc' (Just bnds) = do
  addKernels kernels
  addKernel $ fmap (certify cs) bnds
  return acc'
