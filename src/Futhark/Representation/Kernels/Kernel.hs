{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Futhark.Representation.Kernels.Kernel
       ( Kernel(..)
       , kernelType
       , kernelSpace
       , KernelDebugHints(..)
       , GenReduceOp(..)
       , SegRedOp(..)
       , segRedResults
       , KernelBody(..)
       , KernelSpace(..)
       , spaceDimensions
       , SpaceStructure(..)
       , scopeOfKernelSpace
       , KernelResult(..)
       , kernelResultSubExp
       , KernelPath

       , chunkedKernelNonconcatOutputs

       , typeCheckKernel

         -- * Generic traversal
       , KernelMapper(..)
       , identityKernelMapper
       , mapKernelM
       , KernelWalker(..)
       , identityKernelWalker
       , walkKernelM

       -- * Host operations
       , HostOp(..)
       , typeCheckHostOp
       )
       where

import Control.Arrow (first)
import Control.Monad.Writer hiding (mapM_)
import Control.Monad.Identity hiding (mapM_)
import qualified Data.Set as S
import qualified Data.Map.Strict as M
import Data.Foldable
import Data.List

import Futhark.Representation.AST
import qualified Futhark.Analysis.Alias as Alias
import qualified Futhark.Analysis.SymbolTable as ST
import Futhark.Analysis.PrimExp.Convert
import qualified Futhark.Util.Pretty as PP
import Futhark.Util.Pretty
  ((</>), (<+>), ppr, commasep, Pretty, parens, text)
import Futhark.Transform.Substitute
import Futhark.Transform.Rename
import Futhark.Optimise.Simplify.Lore
import Futhark.Representation.Ranges
  (Ranges, removeLambdaRanges, removeBodyRanges, mkBodyRanges)
import Futhark.Representation.AST.Attributes.Ranges
import Futhark.Representation.AST.Attributes.Aliases
import Futhark.Representation.Aliases
  (Aliases, removeLambdaAliases, removeBodyAliases, removeStmAliases)
import Futhark.Representation.Kernels.KernelExp (SplitOrdering(..))
import Futhark.Representation.Kernels.Sizes
import qualified Futhark.TypeCheck as TC
import Futhark.Analysis.Metrics
import Futhark.Tools (partitionChunkedKernelLambdaParameters)
import qualified Futhark.Analysis.Range as Range
import Futhark.Util (maybeNth)

-- | Some information about what goes into a kernel, and where it came
-- from.  Has no semantic meaning; only used for debugging generated
-- code.
data KernelDebugHints =
  KernelDebugHints { kernelName :: String
                   , kernelHints :: [(String, SubExp)]
                     -- ^ A mapping from a description to some
                     -- PrimType value.
                   }
  deriving (Eq, Show, Ord)

data GenReduceOp lore =
  GenReduceOp { genReduceWidth :: SubExp
              , genReduceDest :: [VName]
              , genReduceNeutral :: [SubExp]
              , genReduceShape :: Shape
                -- ^ In case this operator is semantically a
                -- vectorised operator (corresponding to a perfect map
                -- nest in the SOACS representation), these are the
                -- logical "dimensions".  This is used to generate
                -- more efficient code.
              , genReduceOp :: LambdaT lore
              }
  deriving (Eq, Ord, Show)

data SegRedOp lore =
  SegRedOp { segRedComm :: Commutativity
           , segRedLambda :: Lambda lore
           , segRedNeutral :: [SubExp]
           , segRedShape :: Shape
             -- ^ In case this operator is semantically a vectorised
             -- operator (corresponding to a perfect map nest in the
             -- SOACS representation), these are the logical
             -- "dimensions".  This is used to generate more efficient
             -- code.
           }
  deriving (Eq, Ord, Show)

-- | How many reduction results are produced by these 'SegRedOp's?
segRedResults :: [SegRedOp lore] -> Int
segRedResults = sum . map (length . segRedNeutral)

data Kernel lore
  = Kernel KernelDebugHints KernelSpace [Type] (KernelBody lore)
  | SegMap KernelSpace [Type] (KernelBody lore)
  | SegRed KernelSpace [SegRedOp lore] [Type] (KernelBody lore)
    -- ^ The KernelSpace must always have at least two dimensions,
    -- implying that the result of a SegRed is always an array.
  | SegScan KernelSpace (Lambda lore) [SubExp] [Type] (KernelBody lore)
  | SegGenRed KernelSpace [GenReduceOp lore] [Type] (KernelBody lore)
    deriving (Eq, Show, Ord)

kernelSpace :: Kernel lore -> KernelSpace
kernelSpace (Kernel _ kspace _ _) = kspace
kernelSpace (SegMap kspace _ _) = kspace
kernelSpace (SegRed kspace _ _ _) = kspace
kernelSpace (SegScan kspace _ _ _ _) = kspace
kernelSpace (SegGenRed kspace _ _ _) = kspace

data KernelSpace = KernelSpace { spaceGlobalId :: VName
                               , spaceLocalId :: VName
                               , spaceGroupId :: VName
                               , spaceNumThreads :: SubExp
                               , spaceNumGroups :: SubExp
                               , spaceGroupSize :: SubExp -- flat group size
                               , spaceNumVirtGroups :: SubExp
                                 -- How many groups should we pretend
                                 -- exist?
                               , spaceStructure :: SpaceStructure
                               -- TODO: document what this spaceStructure is
                               -- used for
                               }
                 deriving (Eq, Show, Ord)
-- ^ first three bound in the kernel, the rest are params to kernel

-- | Indices computed for each thread (or group) inside the kernel.
-- This is an arbitrary-dimensional space that is generated from the
-- flat GPU thread space.
data SpaceStructure = FlatThreadSpace
                      [(VName, SubExp)] -- gtids and dim sizes
                    | NestedThreadSpace
                      [(VName, -- gtid
                        SubExp, -- global dim size
                        VName, -- ltid
                        SubExp -- local dim sizes
                       )]
                    deriving (Eq, Show, Ord)

-- | Global thread IDs and their upper bound.
spaceDimensions :: KernelSpace -> [(VName, SubExp)]
spaceDimensions = structureDimensions . spaceStructure
  where structureDimensions (FlatThreadSpace dims) = dims
        structureDimensions (NestedThreadSpace dims) =
          let (gtids, gdim_sizes, _, _) = unzip4 dims
          in zip gtids gdim_sizes

-- | The body of a 'Kernel'.
data KernelBody lore = KernelBody { kernelBodyLore :: BodyAttr lore
                                  , kernelBodyStms :: Stms lore
                                  , kernelBodyResult :: [KernelResult]
                                  }

deriving instance Annotations lore => Ord (KernelBody lore)
deriving instance Annotations lore => Show (KernelBody lore)
deriving instance Annotations lore => Eq (KernelBody lore)

data KernelResult = ThreadsReturn SubExp
                    -- ^ Each thread in the kernel space (which must
                    -- be non-empty) returns this.
                  | GroupsReturn SubExp
                    -- ^ Each group returns this.
                  | WriteReturn
                    [SubExp] -- Size of array.  Must match number of dims.
                    VName -- Which array
                    [([SubExp], SubExp)]
                    -- Arbitrary number of index/value pairs.
                  | ConcatReturns
                    SplitOrdering -- Permuted?
                    SubExp -- The final size.
                    SubExp -- Per-thread (max) chunk size.
                    (Maybe SubExp) -- Optional precalculated offset.
                    VName -- Chunk by this thread.
                  deriving (Eq, Show, Ord)

kernelResultSubExp :: KernelResult -> SubExp
kernelResultSubExp (ThreadsReturn se) = se
kernelResultSubExp (GroupsReturn se) = se
kernelResultSubExp (WriteReturn _ arr _) = Var arr
kernelResultSubExp (ConcatReturns _ _ _ _ v) = Var v

-- | Like 'Mapper', but just for 'Kernel's.
data KernelMapper flore tlore m = KernelMapper {
    mapOnKernelSubExp :: SubExp -> m SubExp
  , mapOnKernelLambda :: Lambda flore -> m (Lambda tlore)
  , mapOnKernelBody :: Body flore -> m (Body tlore)
  , mapOnKernelVName :: VName -> m VName
  , mapOnKernelLParam :: LParam flore -> m (LParam tlore)
  , mapOnKernelKernelBody :: KernelBody flore -> m (KernelBody tlore)
  }

-- | A mapper that simply returns the 'Kernel' verbatim.
identityKernelMapper :: Monad m => KernelMapper lore lore m
identityKernelMapper = KernelMapper { mapOnKernelSubExp = return
                                    , mapOnKernelLambda = return
                                    , mapOnKernelBody = return
                                    , mapOnKernelVName = return
                                    , mapOnKernelLParam = return
                                    , mapOnKernelKernelBody = return
                                    }

-- | Map a monadic action across the immediate children of a
-- Kernel.  The mapping does not descend recursively into subexpressions
-- and is done left-to-right.
mapKernelM :: (Applicative m, Monad m) =>
              KernelMapper flore tlore m -> Kernel flore -> m (Kernel tlore)
mapKernelM tv (SegMap space ts body) =
  SegMap
  <$> mapOnKernelSpace tv space
  <*> mapM (mapOnType $ mapOnKernelSubExp tv) ts
  <*> mapOnKernelKernelBody tv body
mapKernelM tv (SegRed space reds ts body) =
  SegRed
  <$> mapOnKernelSpace tv space
  <*> mapM onSegOp reds
  <*> mapM (mapOnType $ mapOnKernelSubExp tv) ts
  <*> mapOnKernelKernelBody tv body
  where onSegOp (SegRedOp comm red_op nes shape) =
          SegRedOp comm
          <$> mapOnKernelLambda tv red_op
          <*> mapM (mapOnKernelSubExp tv) nes
          <*> (Shape <$> mapM (mapOnKernelSubExp tv) (shapeDims shape))
mapKernelM tv (SegScan space scan_op nes ts body) =
  SegScan
  <$> mapOnKernelSpace tv space
  <*> mapOnKernelLambda tv scan_op
  <*> mapM (mapOnKernelSubExp tv) nes
  <*> mapM (mapOnType $ mapOnKernelSubExp tv) ts
  <*> mapOnKernelKernelBody tv body
mapKernelM tv (SegGenRed space ops ts body) =
  SegGenRed
  <$> mapOnKernelSpace tv space
  <*> mapM onGenRedOp ops
  <*> mapM (mapOnType $ mapOnKernelSubExp tv) ts
  <*> mapOnKernelKernelBody tv body
  where onGenRedOp (GenReduceOp w arrs nes shape op) =
          GenReduceOp <$> mapOnKernelSubExp tv w
          <*> mapM (mapOnKernelVName tv) arrs
          <*> mapM (mapOnKernelSubExp tv) nes
          <*> (Shape <$> mapM (mapOnKernelSubExp tv) (shapeDims shape))
          <*> mapOnKernelLambda tv op
mapKernelM tv (Kernel desc space ts kernel_body) =
  Kernel <$> mapOnKernelDebugHints desc <*>
  mapOnKernelSpace tv space <*>
  mapM (mapOnKernelType tv) ts <*>
  mapOnKernelKernelBody tv kernel_body
  where mapOnKernelDebugHints (KernelDebugHints name kvs) =
          KernelDebugHints name <$>
          (zip (map fst kvs) <$> mapM (mapOnKernelSubExp tv . snd) kvs)

mapOnKernelSpace :: Monad f =>
                    KernelMapper flore tlore f -> KernelSpace -> f KernelSpace
mapOnKernelSpace tv (KernelSpace gtid ltid gid num_threads num_groups group_size virt_groups structure) =
  KernelSpace gtid ltid gid -- all in binding position
  <$> mapOnKernelSubExp tv num_threads
  <*> mapOnKernelSubExp tv num_groups
  <*> mapOnKernelSubExp tv group_size
  <*> mapOnKernelSubExp tv virt_groups
  <*> mapOnKernelStructure structure
  where mapOnKernelStructure (FlatThreadSpace dims) =
          FlatThreadSpace <$> (zip gtids <$> mapM (mapOnKernelSubExp tv) gdim_sizes)
          where (gtids, gdim_sizes) = unzip dims
        mapOnKernelStructure (NestedThreadSpace dims) =
          NestedThreadSpace <$> (zip4 gtids
                                 <$> mapM (mapOnKernelSubExp tv) gdim_sizes
                                 <*> pure ltids
                                 <*> mapM (mapOnKernelSubExp tv) ldim_sizes)
          where (gtids, gdim_sizes, ltids, ldim_sizes) = unzip4 dims

mapOnKernelType :: Monad m =>
                   KernelMapper flore tlore m -> Type -> m Type
mapOnKernelType _tv (Prim pt) = pure $ Prim pt
mapOnKernelType tv (Array pt shape u) = Array pt <$> f shape <*> pure u
  where f (Shape dims) = Shape <$> mapM (mapOnKernelSubExp tv) dims
mapOnKernelType _tv (Mem s) = pure $ Mem s

instance (Attributes lore, FreeIn (LParamAttr lore)) =>
         FreeIn (Kernel lore) where
  freeIn e = execWriter $ mapKernelM free e
    where walk f x = tell (f x) >> return x
          free = KernelMapper { mapOnKernelSubExp = walk freeIn
                              , mapOnKernelLambda = walk freeIn
                              , mapOnKernelBody = walk freeIn
                              , mapOnKernelVName = walk freeIn
                              , mapOnKernelLParam = walk freeIn
                              , mapOnKernelKernelBody = walk freeIn
                              }

-- | Like 'Walker', but just for 'Kernel's.
data KernelWalker lore m = KernelWalker {
    walkOnKernelSubExp :: SubExp -> m ()
  , walkOnKernelLambda :: Lambda lore -> m ()
  , walkOnKernelBody :: Body lore -> m ()
  , walkOnKernelVName :: VName -> m ()
  , walkOnKernelLParam :: LParam lore -> m ()
  , walkOnKernelKernelBody :: KernelBody lore -> m ()
  }

-- | A no-op traversal.
identityKernelWalker :: Monad m => KernelWalker lore m
identityKernelWalker = KernelWalker {
    walkOnKernelSubExp = const $ return ()
  , walkOnKernelLambda = const $ return ()
  , walkOnKernelBody = const $ return ()
  , walkOnKernelVName = const $ return ()
  , walkOnKernelLParam = const $ return ()
  , walkOnKernelKernelBody = const $ return ()
  }

walkKernelMapper :: forall lore m. Monad m =>
                    KernelWalker lore m -> KernelMapper lore lore m
walkKernelMapper f = KernelMapper {
    mapOnKernelSubExp = wrap walkOnKernelSubExp
  , mapOnKernelLambda = wrap walkOnKernelLambda
  , mapOnKernelBody = wrap walkOnKernelBody
  , mapOnKernelVName = wrap walkOnKernelVName
  , mapOnKernelLParam = wrap walkOnKernelLParam
  , mapOnKernelKernelBody = wrap walkOnKernelKernelBody
  }
  where wrap :: (KernelWalker lore m -> a -> m ()) -> a -> m a
        wrap op k = op f k >> return k

-- | As 'mapKernelM', but ignoring the results.
walkKernelM :: Monad m => KernelWalker lore m -> Kernel lore -> m ()
walkKernelM f = void . mapKernelM m
  where m = walkKernelMapper f

instance FreeIn KernelResult where
  freeIn (GroupsReturn what) = freeIn what
  freeIn (ThreadsReturn what) = freeIn what
  freeIn (WriteReturn rws arr res) = freeIn rws <> freeIn arr <> freeIn res
  freeIn (ConcatReturns o w per_thread_elems moffset v) =
    freeIn o <> freeIn w <> freeIn per_thread_elems <> freeIn moffset <> freeIn v

instance Attributes lore => FreeIn (KernelBody lore) where
  freeIn (KernelBody attr stms res) =
    (freeIn attr <> free_in_stms <> free_in_res) `S.difference` bound_in_stms
    where free_in_stms = fold $ fmap freeIn stms
          free_in_res = freeIn res
          bound_in_stms = fold $ fmap boundByStm stms

instance Attributes lore => Substitute (KernelBody lore) where
  substituteNames subst (KernelBody attr stms res) =
    KernelBody
    (substituteNames subst attr)
    (substituteNames subst stms)
    (substituteNames subst res)

instance Substitute KernelResult where
  substituteNames subst (GroupsReturn se) =
    GroupsReturn $ substituteNames subst se
  substituteNames subst (ThreadsReturn se) =
    ThreadsReturn $ substituteNames subst se
  substituteNames subst (WriteReturn rws arr res) =
    WriteReturn
    (substituteNames subst rws) (substituteNames subst arr)
    (substituteNames subst res)
  substituteNames subst (ConcatReturns o w per_thread_elems moffset v) =
    ConcatReturns
    (substituteNames subst o)
    (substituteNames subst w)
    (substituteNames subst per_thread_elems)
    (substituteNames subst moffset)
    (substituteNames subst v)

instance Substitute KernelSpace where
  substituteNames subst (KernelSpace gtid ltid gid num_threads num_groups group_size virt_groups structure) =
    KernelSpace (substituteNames subst gtid)
    (substituteNames subst ltid)
    (substituteNames subst gid)
    (substituteNames subst num_threads)
    (substituteNames subst num_groups)
    (substituteNames subst group_size)
    (substituteNames subst virt_groups)
    (substituteNames subst structure)

instance Substitute SpaceStructure where
  substituteNames subst (FlatThreadSpace dims) =
    FlatThreadSpace (map (substituteNames subst) dims)
  substituteNames subst (NestedThreadSpace dims) =
    NestedThreadSpace (map (substituteNames subst) dims)

instance Attributes lore => Substitute (Kernel lore) where
  substituteNames subst (Kernel desc space ts kbody) =
    Kernel desc
    (substituteNames subst space)
    (substituteNames subst ts)
    (substituteNames subst kbody)
  substituteNames subst k = runIdentity $ mapKernelM substitute k
    where substitute =
            KernelMapper { mapOnKernelSubExp = return . substituteNames subst
                         , mapOnKernelLambda = return . substituteNames subst
                         , mapOnKernelBody = return . substituteNames subst
                         , mapOnKernelVName = return . substituteNames subst
                         , mapOnKernelLParam = return . substituteNames subst
                         , mapOnKernelKernelBody = return . substituteNames subst
                         }

instance Attributes lore => Rename (KernelBody lore) where
  rename (KernelBody attr stms res) = do
    attr' <- rename attr
    renamingStms stms $ \stms' ->
      KernelBody attr' stms' <$> rename res

instance Rename KernelResult where
  rename = substituteRename

scopeOfKernelSpace :: KernelSpace -> Scope lore
scopeOfKernelSpace (KernelSpace gtid ltid gid _ _ _ _ structure) =
  M.fromList $ zip ([gtid, ltid, gid] ++ structure') $ repeat $ IndexInfo Int32
  where structure' = case structure of
                       FlatThreadSpace dims -> map fst dims
                       NestedThreadSpace dims ->
                         let (gtids, _, ltids, _) = unzip4 dims
                         in gtids ++ ltids

instance Attributes lore => Rename (Kernel lore) where
  rename = mapKernelM renamer
    where renamer = KernelMapper rename rename rename rename rename rename


kernelResultShape :: KernelSpace -> Type -> KernelResult -> Type
kernelResultShape _ t (WriteReturn rws _ _) =
  t `arrayOfShape` Shape rws
kernelResultShape space t (GroupsReturn _) =
  t `arrayOfRow` spaceNumGroups space
kernelResultShape space t (ThreadsReturn _) =
  foldr (flip arrayOfRow . snd) t $ spaceDimensions space
kernelResultShape _ t (ConcatReturns _ w _ _ _) =
  t `arrayOfRow` w

kernelType :: Kernel lore -> [Type]
kernelType (Kernel _ space ts body) =
  zipWith (kernelResultShape space) ts $ kernelBodyResult body

kernelType (SegMap space ts body) =
  zipWith (kernelResultShape space) ts $ kernelBodyResult body

kernelType (SegRed space reds ts body) =
  red_ts ++
  zipWith (kernelResultShape space) map_ts
  (drop (length red_ts) $ kernelBodyResult body)
  where map_ts = drop (length red_ts) ts
        segment_dims = init $ map snd $ spaceDimensions space
        red_ts = do
          op <- reds
          let shape = Shape segment_dims <> segRedShape op
          map (`arrayOfShape` shape) (lambdaReturnType $ segRedLambda op)

kernelType (SegScan space _ _ ts _) =
  map (`arrayOfShape` Shape dims) ts
  where dims = map snd $ spaceDimensions space

kernelType (SegGenRed space ops _ _) = do
  op <- ops
  let shape = Shape (segment_dims <> [genReduceWidth op]) <> genReduceShape op
  map (`arrayOfShape` shape) (lambdaReturnType $ genReduceOp op)
  where dims = map snd $ spaceDimensions space
        segment_dims = init dims

chunkedKernelNonconcatOutputs :: Lambda lore -> Int
chunkedKernelNonconcatOutputs fun =
  length $ takeWhile (not . outerSizeIsChunk) $ lambdaReturnType fun
  where outerSizeIsChunk = (==Var (paramName chunk)) . arraySize 0
        (_, chunk, _) = partitionChunkedKernelLambdaParameters $ lambdaParams fun

instance TypedOp (Kernel lore) where
  opType = pure . staticShapes . kernelType

instance (Attributes lore, Aliased lore) => AliasedOp (Kernel lore) where
  opAliases = map (const mempty) . kernelType

  consumedInOp (Kernel _ _ _ kbody) =
    consumedInKernelBody kbody <>
    mconcat (map consumedByReturn (kernelBodyResult kbody))
    where consumedByReturn (WriteReturn _ a _) = S.singleton a
          consumedByReturn _                   = mempty
  consumedInOp (SegGenRed _ ops _ kbody) =
    S.fromList (concatMap genReduceDest ops) <>
    consumedInKernelBody kbody
  consumedInOp (SegMap _ _ kbody) =
    consumedInKernelBody kbody
  consumedInOp (SegRed _ _ _ kbody) =
    consumedInKernelBody kbody
  consumedInOp (SegScan _ _ _ _ kbody) =
    consumedInKernelBody kbody

aliasAnalyseKernelBody :: (Attributes lore,
                           CanBeAliased (Op lore)) =>
                          KernelBody lore
                       -> KernelBody (Aliases lore)
aliasAnalyseKernelBody (KernelBody attr stms res) =
  let Body attr' stms' _ = Alias.analyseBody $ Body attr stms []
  in KernelBody attr' stms' res

instance (Attributes lore,
          Attributes (Aliases lore),
          CanBeAliased (Op lore)) => CanBeAliased (Kernel lore) where
  type OpWithAliases (Kernel lore) = Kernel (Aliases lore)

  addOpAliases = runIdentity . mapKernelM alias
    where alias = KernelMapper return (return . Alias.analyseLambda)
                  (return . Alias.analyseBody) return return
                  (return . aliasAnalyseKernelBody)

  removeOpAliases = runIdentity . mapKernelM remove
    where remove = KernelMapper return (return . removeLambdaAliases)
                   (return . removeBodyAliases) return return
                   (return . removeKernelBodyAliases)
          removeKernelBodyAliases :: KernelBody (Aliases lore)
                                  -> KernelBody lore
          removeKernelBodyAliases (KernelBody (_, attr) stms res) =
            KernelBody attr (fmap removeStmAliases stms) res

instance Attributes lore => IsOp (Kernel lore) where
  safeOp _ = True
  cheapOp Kernel{} = False
  cheapOp _ = True

instance Ranged inner => RangedOp (Kernel inner) where
  opRanges op = replicate (length $ kernelType op) unknownRange

instance (Attributes lore, CanBeRanged (Op lore)) => CanBeRanged (Kernel lore) where
  type OpWithRanges (Kernel lore) = Kernel (Ranges lore)

  removeOpRanges = runIdentity . mapKernelM remove
    where remove = KernelMapper return (return . removeLambdaRanges)
                   (return . removeBodyRanges) return return
                   (return . removeKernelBodyRanges)
          removeKernelBodyRanges = error "removeKernelBodyRanges"
  addOpRanges = Range.runRangeM . mapKernelM add
    where add = KernelMapper return Range.analyseLambda
                Range.analyseBody return return addKernelBodyRanges
          addKernelBodyRanges (KernelBody attr stms res) =
            Range.analyseStms stms $ \stms' -> do
            let attr' = (mkBodyRanges stms $ map kernelResultSubExp res, attr)
            return $ KernelBody attr' stms' res

instance (Attributes lore, CanBeWise (Op lore)) => CanBeWise (Kernel lore) where
  type OpWithWisdom (Kernel lore) = Kernel (Wise lore)

  removeOpWisdom = runIdentity . mapKernelM remove
    where remove = KernelMapper return
                   (return . removeLambdaWisdom)
                   (return . removeBodyWisdom)
                   return return
                   (return . removeKernelBodyWisdom)
          removeKernelBodyWisdom :: KernelBody (Wise lore)
                                 -> KernelBody lore
          removeKernelBodyWisdom (KernelBody attr stms res) =
            let Body attr' stms' _ = removeBodyWisdom $ Body attr stms []
            in KernelBody attr' stms' res

instance Attributes lore => ST.IndexOp (Kernel lore) where
  indexOp vtable k (Kernel _ space _ kbody) is = do
    ThreadsReturn se <- maybeNth k $ kernelBodyResult kbody
    let (gtids, _) = unzip $ spaceDimensions space
    guard $ length gtids == length is
    let prim_table = M.fromList $ zip gtids $ zip is $ repeat mempty
        prim_table' = foldl expandPrimExpTable prim_table $ kernelBodyStms kbody
    case se of
      Var v -> M.lookup v prim_table'
      _ -> Nothing
    where expandPrimExpTable table stm
            | [v] <- patternNames $ stmPattern stm,
              Just (pe,cs) <-
                  runWriterT $ primExpFromExp (asPrimExp table) $ stmExp stm =
                M.insert v (pe, stmCerts stm <> cs) table
            | otherwise =
                table

          asPrimExp table v
            | Just (e,cs) <- M.lookup v table = tell cs >> return e
            | Just (Prim pt) <- ST.lookupType v vtable =
                return $ LeafExp v pt
            | otherwise = lift Nothing

  indexOp _ _ _ _ = Nothing

consumedInKernelBody :: Aliased lore =>
                        KernelBody lore -> Names
consumedInKernelBody (KernelBody attr stms _) =
  consumedInBody $ Body attr stms []

typeCheckKernel :: TC.Checkable lore => Kernel (Aliases lore) -> TC.TypeM lore ()

typeCheckKernel (SegMap space ts kbody) = do
  checkSpace space
  mapM_ TC.checkType ts
  TC.binding (scopeOfKernelSpace space) $ checkKernelBody ts kbody

typeCheckKernel (SegRed space reds ts body) =
  checkScanRed space reds' ts body
  where reds' = zip3
                (map segRedLambda reds)
                (map segRedNeutral reds)
                (map segRedShape reds)

typeCheckKernel (SegScan space scan_op nes ts body) =
  checkScanRed space [(scan_op, nes, mempty)] ts body

typeCheckKernel (SegGenRed space ops ts body) = do
  checkSpace space
  mapM_ TC.checkType ts

  TC.binding (scopeOfKernelSpace space) $ do
    nes_ts <- forM ops $ \(GenReduceOp dest_w dests nes shape op) -> do
      TC.require [Prim int32] dest_w
      nes' <- mapM TC.checkArg nes
      mapM_ (TC.require [Prim int32]) $ shapeDims shape

      -- Operator type must match the type of neutral elements.
      let stripVecDims = stripArray $ shapeRank shape
      TC.checkLambda op $ map (TC.noArgAliases . first stripVecDims) $ nes' ++ nes'
      let nes_t = map TC.argType nes'
      unless (nes_t == lambdaReturnType op) $
        TC.bad $ TC.TypeError $ "SegGenRed operator has return type " ++
        prettyTuple (lambdaReturnType op) ++ " but neutral element has type " ++
        prettyTuple nes_t

      -- Arrays must have proper type.
      let dest_shape = Shape (segment_dims <> [dest_w]) <> shape
      forM_ (zip nes_t dests) $ \(t, dest) -> do
        TC.requireI [t `arrayOfShape` dest_shape] dest
        TC.consume =<< TC.lookupAliases dest

      return $ map (`arrayOfShape` shape) nes_t

    checkKernelBody ts body

    -- Return type of bucket function must be an index for each
    -- operation followed by the values to write.
    let bucket_ret_t = replicate (length ops) (Prim int32) ++ concat nes_ts
    unless (bucket_ret_t == ts) $
      TC.bad $ TC.TypeError $ "SegGenRed body has return type " ++
      prettyTuple ts ++ " but should have type " ++
      prettyTuple bucket_ret_t

  where segment_dims = init $ map snd $ spaceDimensions space

typeCheckKernel (Kernel _ space kts kbody) = do
  checkSpace space
  mapM_ TC.checkType kts
  mapM_ (TC.require [Prim int32] . snd) $ spaceDimensions space

  TC.binding (scopeOfKernelSpace space) $
    checkKernelBody kts kbody

checkKernelBody :: TC.Checkable lore =>
                   [Type] -> KernelBody (Aliases lore) -> TC.TypeM lore ()
checkKernelBody ts (KernelBody (_, attr) stms kres) = do
  TC.checkBodyLore attr
  TC.checkStms stms $ do
    unless (length ts == length kres) $
      TC.bad $ TC.TypeError $ "Kernel return type is " ++ prettyTuple ts ++
      ", but body returns " ++ show (length kres) ++ " values."
    zipWithM_ checkKernelResult kres ts

  where checkKernelResult (GroupsReturn what) t =
          TC.require [t] what
        checkKernelResult (ThreadsReturn what) t =
          TC.require [t] what
        checkKernelResult (WriteReturn rws arr res) t = do
          mapM_ (TC.require [Prim int32]) rws
          arr_t <- lookupType arr
          forM_ res $ \(is, e) -> do
            mapM_ (TC.require [Prim int32]) is
            TC.require [t] e
            unless (arr_t == t `arrayOfShape` Shape rws) $
              TC.bad $ TC.TypeError $ "WriteReturn returning " ++
              pretty e ++ " of type " ++ pretty t ++ ", shape=" ++ pretty rws ++
              ", but destination array has type " ++ pretty arr_t
          TC.consume =<< TC.lookupAliases arr
        checkKernelResult (ConcatReturns o w per_thread_elems moffset v) t = do
          case o of
            SplitContiguous     -> return ()
            SplitStrided stride -> TC.require [Prim int32] stride
          TC.require [Prim int32] w
          TC.require [Prim int32] per_thread_elems
          mapM_ (TC.require [Prim int32]) moffset
          vt <- lookupType v
          unless (vt == t `arrayOfRow` arraySize 0 vt) $
            TC.bad $ TC.TypeError $ "Invalid type for ConcatReturns " ++ pretty v

checkScanRed :: TC.Checkable lore =>
                KernelSpace
             -> [(Lambda (Aliases lore), [SubExp], Shape)]
             -> [Type]
             -> KernelBody (Aliases lore)
             -> TC.TypeM lore ()
checkScanRed space ops ts kbody = do
  checkSpace space
  mapM_ TC.checkType ts

  TC.binding (scopeOfKernelSpace space) $ do
    ne_ts <- forM ops $ \(lam, nes, shape) -> do
      mapM_ (TC.require [Prim int32]) $ shapeDims shape
      nes' <- mapM TC.checkArg nes

      -- Operator type must match the type of neutral elements.
      let stripVecDims = stripArray $ shapeRank shape
      TC.checkLambda lam $ map (TC.noArgAliases . first stripVecDims) $ nes' ++ nes'
      let nes_t = map TC.argType nes'

      unless (lambdaReturnType lam == nes_t) $
        TC.bad $ TC.TypeError "wrong type for operator or neutral elements."

      return $ map (`arrayOfShape` shape) nes_t

    let expecting = concat ne_ts
        got = take (length expecting) ts
    unless (expecting == got) $
      TC.bad $ TC.TypeError $
      "Wrong return for body (does not match neutral elements; expected " ++
      pretty expecting ++ "; found " ++
      pretty got ++ ")"

    checkKernelBody ts kbody

checkSpace :: TC.Checkable lore => KernelSpace -> TC.TypeM lore ()
checkSpace (KernelSpace _ _ _ num_threads num_groups group_size virt_groups structure) = do
  mapM_ (TC.require [Prim int32]) [num_threads,num_groups,group_size,virt_groups]
  case structure of
    FlatThreadSpace dims ->
      mapM_ (TC.require [Prim int32] . snd) dims
    NestedThreadSpace dims ->
      let (_, gdim_sizes, _, ldim_sizes) = unzip4 dims
      in mapM_ (TC.require [Prim int32]) $ gdim_sizes ++ ldim_sizes

instance OpMetrics (Op lore) => OpMetrics (Kernel lore) where
  opMetrics (Kernel _ _ _ kbody) =
    inside "Kernel" $ kernelBodyMetrics kbody
  opMetrics (SegMap _ _ body) =
    inside "SegMap" $ kernelBodyMetrics body
  opMetrics (SegRed _ reds _ body) =
    inside "SegRed" $ do mapM_ (lambdaMetrics . segRedLambda) reds
                         kernelBodyMetrics body
  opMetrics (SegScan _ scan_op _ _ body) =
    inside "SegScan" $ lambdaMetrics scan_op >> kernelBodyMetrics body
  opMetrics (SegGenRed _ ops _ body) =
    inside "SegGenRed" $ do mapM_ (lambdaMetrics . genReduceOp) ops
                            kernelBodyMetrics body

kernelBodyMetrics :: OpMetrics (Op lore) => KernelBody lore -> MetricsM ()
kernelBodyMetrics = mapM_ bindingMetrics . kernelBodyStms

instance PrettyLore lore => PP.Pretty (Kernel lore) where
  ppr (Kernel desc space ts body) =
    text "kernel" <+> text (kernelName desc) <>
    PP.align (ppr space) <+>
    PP.colon <+> ppTuple' ts <+> PP.nestedBlock "{" "}" (ppr body)

  ppr (SegMap space ts body) =
    text "segmap" <>
    PP.align (ppr space) <+> PP.colon <+> ppTuple' ts <+>
    PP.nestedBlock "{" "}" (ppr body)

  ppr (SegRed space reds ts body) =
    text "segred" <>
    PP.parens (PP.braces (mconcat $ intersperse (PP.comma <> PP.line) $ map ppOp reds)) </>
    PP.align (ppr space) <+> PP.colon <+> ppTuple' ts <+>
    PP.nestedBlock "{" "}" (ppr body)
    where ppOp (SegRedOp comm lam nes shape) =
            PP.braces (PP.commasep $ map ppr nes) <> PP.comma </>
            ppr shape <> PP.comma </>
            comm' <> ppr lam
            where comm' = case comm of Commutative -> text "commutative "
                                       Noncommutative -> mempty


  ppr (SegScan space scan_op nes ts body) =
    text "segscan" <> PP.parens (ppr scan_op <> PP.comma </>
                                 PP.braces (PP.commasep $ map ppr nes)) </>
    PP.align (ppr space) <+> PP.colon <+> ppTuple' ts <+>
    PP.nestedBlock "{" "}" (ppr body)

  ppr (SegGenRed space ops ts body) =
    text "seggenred" <>
    PP.parens (PP.braces (mconcat $ intersperse (PP.comma <> PP.line) $ map ppOp ops)) </>
    PP.align (ppr space) <+> PP.colon <+> ppTuple' ts <+>
    PP.nestedBlock "{" "}" (ppr body)
    where ppOp (GenReduceOp w dests nes shape op) =
            ppr w <> PP.comma </>
            PP.braces (PP.commasep $ map ppr dests) <> PP.comma </>
            PP.braces (PP.commasep $ map ppr nes) <> PP.comma </>
            ppr shape <> PP.comma </>
            ppr op

instance Pretty KernelSpace where
  ppr (KernelSpace f_gtid f_ltid gid num_threads num_groups group_size virt_groups structure) =
    parens (commasep [text "num groups:" <+> ppr num_groups,
                      text "group size:" <+> ppr group_size,
                      text "virt_num_groups:" <+> ppr virt_groups,
                      text "num threads:" <+> ppr num_threads,
                      text "global TID ->" <+> ppr f_gtid,
                      text "local TID ->" <+> ppr f_ltid,
                      text "group ID ->" <+> ppr gid]) </> structure'
    where structure' =
            case structure of
              FlatThreadSpace dims -> flat dims
              NestedThreadSpace space ->
                parens (commasep $ do
                           (gtid,gd,ltid,ld) <- space
                           return $ ppr (gtid,ltid) <+> "<" <+> ppr (gd,ld))
          flat dims = parens $ commasep $ do
            (i,d) <- dims
            return $ ppr i <+> "<" <+> ppr d

instance PrettyLore lore => Pretty (KernelBody lore) where
  ppr (KernelBody _ stms res) =
    PP.stack (map ppr (stmsToList stms)) </>
    text "return" <+> PP.braces (PP.commasep $ map ppr res)

instance Pretty KernelResult where
  ppr (GroupsReturn what) =
    text "group returns" <+> ppr what
  ppr (ThreadsReturn what) =
    text "thread returns" <+> ppr what
  ppr (WriteReturn rws arr res) =
    ppr arr <+> text "with" <+> PP.apply (map ppRes res)
    where ppRes (is, e) =
            PP.brackets (PP.commasep $ zipWith f is rws) <+> text "<-" <+> ppr e
          f i rw = ppr i <+> text "<" <+> ppr rw
  ppr (ConcatReturns o w per_thread_elems offset v) =
    text "concat" <> suff <>
    parens (commasep [ppr w, ppr per_thread_elems] <> offset_text) <+>
    ppr v
    where suff = case o of SplitContiguous     -> mempty
                           SplitStrided stride -> text "Strided" <> parens (ppr stride)
          offset_text = case offset of Nothing -> ""
                                       Just se -> "," <+> "offset=" <> ppr se

--- Host operations

-- | A host-level operation; parameterised by what else it can do.
data HostOp lore inner
  = GetSize Name SizeClass
    -- ^ Produce some runtime-configurable size.
  | GetSizeMax SizeClass
    -- ^ The maximum size of some class.
  | CmpSizeLe Name SizeClass SubExp
    -- ^ Compare size (likely a threshold) with some Int32 value.
  | HostOp inner
    -- ^ The arbitrary operation.
  deriving (Eq, Ord, Show)

instance Substitute inner => Substitute (HostOp lore inner) where
  substituteNames substs (HostOp op) =
    HostOp $ substituteNames substs op
  substituteNames substs (CmpSizeLe name sclass x) =
    CmpSizeLe name sclass $ substituteNames substs x
  substituteNames _ x = x

instance Rename inner => Rename (HostOp lore inner) where
  rename (HostOp op) = HostOp <$> rename op
  rename (CmpSizeLe name sclass x) = CmpSizeLe name sclass <$> rename x
  rename x = pure x

instance IsOp inner => IsOp (HostOp lore inner) where
  safeOp (HostOp op) = safeOp op
  safeOp _ = True
  cheapOp (HostOp op) = cheapOp op
  cheapOp _ = True

instance TypedOp inner => TypedOp (HostOp lore inner) where
  opType GetSize{} = pure [Prim int32]
  opType GetSizeMax{} = pure [Prim int32]
  opType CmpSizeLe{} = pure [Prim Bool]
  opType (HostOp op) = opType op

instance AliasedOp inner => AliasedOp (HostOp lore inner) where
  opAliases (HostOp op) = opAliases op
  opAliases _ = [mempty]

  consumedInOp (HostOp op) = consumedInOp op
  consumedInOp _ = mempty

instance RangedOp inner => RangedOp (HostOp lore inner) where
  opRanges (HostOp op) = opRanges op
  opRanges _ = [unknownRange]

instance FreeIn inner => FreeIn (HostOp lore inner) where
  freeIn (HostOp op) = freeIn op
  freeIn (CmpSizeLe _ _ x) = freeIn x
  freeIn _ = mempty

instance CanBeAliased inner => CanBeAliased (HostOp lore inner) where
  type OpWithAliases (HostOp lore inner) = HostOp (Aliases lore) (OpWithAliases inner)

  addOpAliases (HostOp op) = HostOp $ addOpAliases op
  addOpAliases (GetSize name sclass) = GetSize name sclass
  addOpAliases (GetSizeMax sclass) = GetSizeMax sclass
  addOpAliases (CmpSizeLe name sclass x) = CmpSizeLe name sclass x

  removeOpAliases (HostOp op) = HostOp $ removeOpAliases op
  removeOpAliases (GetSize name sclass) = GetSize name sclass
  removeOpAliases (GetSizeMax sclass) = GetSizeMax sclass
  removeOpAliases (CmpSizeLe name sclass x) = CmpSizeLe name sclass x

instance CanBeRanged inner => CanBeRanged (HostOp lore inner) where
  type OpWithRanges (HostOp lore inner) = HostOp (Ranges lore) (OpWithRanges inner)

  addOpRanges (HostOp op) = HostOp $ addOpRanges op
  addOpRanges (GetSize name sclass) = GetSize name sclass
  addOpRanges (GetSizeMax sclass) = GetSizeMax sclass
  addOpRanges (CmpSizeLe name sclass x) = CmpSizeLe name sclass x

  removeOpRanges (HostOp op) = HostOp $ removeOpRanges op
  removeOpRanges (GetSize name sclass) = GetSize name sclass
  removeOpRanges (GetSizeMax sclass) = GetSizeMax sclass
  removeOpRanges (CmpSizeLe name sclass x) = CmpSizeLe name sclass x

instance CanBeWise inner => CanBeWise (HostOp lore inner) where
  type OpWithWisdom (HostOp lore inner) = HostOp (Wise lore) (OpWithWisdom inner)

  removeOpWisdom (HostOp op) = HostOp $ removeOpWisdom op
  removeOpWisdom (GetSize name sclass) = GetSize name sclass
  removeOpWisdom (GetSizeMax sclass) = GetSizeMax sclass
  removeOpWisdom (CmpSizeLe name sclass x) = CmpSizeLe name sclass x

instance ST.IndexOp op => ST.IndexOp (HostOp lore op) where
  indexOp vtable k (HostOp op) is = ST.indexOp vtable k op is
  indexOp _ _ _ _ = Nothing

instance PP.Pretty inner => PP.Pretty (HostOp lore inner) where
  ppr (GetSize name size_class) =
    text "get_size" <> parens (commasep [ppr name, ppr size_class])

  ppr (GetSizeMax size_class) =
    text "get_size_max" <> parens (ppr size_class)

  ppr (CmpSizeLe name size_class x) =
    text "get_size" <> parens (commasep [ppr name, ppr size_class]) <+>
    text "<=" <+> ppr x

  ppr (HostOp op) = ppr op

instance OpMetrics inner => OpMetrics (HostOp lore inner) where
  opMetrics GetSize{} = seen "GetSize"
  opMetrics GetSizeMax{} = seen "GetSizeMax"
  opMetrics CmpSizeLe{} = seen "CmpSizeLe"
  opMetrics (HostOp op) = opMetrics op

typeCheckHostOp :: TC.Checkable lore =>
                   (inner -> TC.TypeM lore ())
                -> HostOp (Aliases lore) inner
                -> TC.TypeM lore ()
typeCheckHostOp _ GetSize{} = return ()
typeCheckHostOp _ GetSizeMax{} = return ()
typeCheckHostOp _ (CmpSizeLe _ _ x) = TC.require [Prim int32] x
typeCheckHostOp f (HostOp op) = f op
