{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TupleSections #-}
module Futhark.Analysis.HORepresentation.MapNest
  ( Nesting (..)
  , MapNest (..)
  , typeOf
  , params
  , inputs
  , setInputs
  , fromSOAC
  , toSOAC
  )
where

import Data.List
import Data.Maybe
import qualified Data.Map.Strict as M
import qualified Data.Set as S

import qualified Futhark.Analysis.HORepresentation.SOAC as SOAC
import Futhark.Analysis.HORepresentation.SOAC (SOAC)

import qualified Futhark.Representation.SOACS.SOAC as Futhark
import Futhark.Transform.Substitute
import Futhark.Representation.AST hiding (typeOf)
import Futhark.MonadFreshNames
import Futhark.Construct

data Nesting lore = Nesting {
    nestingParamNames   :: [VName]
  , nestingResult       :: [VName]
  , nestingReturnType   :: [Type]
  , nestingWidth        :: SubExp
  } deriving (Eq, Ord, Show)

data MapNest lore = MapNest SubExp (Lambda lore) [Nesting lore] [SOAC.Input]
                  deriving (Show)

typeOf :: MapNest lore -> [Type]
typeOf (MapNest w lam [] _) =
  map (`arrayOfRow` w) $ lambdaReturnType lam
typeOf (MapNest w _ (nest:_) _) =
  map (`arrayOfRow` w) $ nestingReturnType nest

params :: MapNest lore -> [VName]
params (MapNest _ lam [] _)       =
  map paramName $ lambdaParams lam
params (MapNest _ _ (nest:_) _) =
  nestingParamNames nest

inputs :: MapNest lore -> [SOAC.Input]
inputs (MapNest _ _ _ inps) = inps

setInputs :: [SOAC.Input] -> MapNest lore -> MapNest lore
setInputs [] (MapNest w body ns _) = MapNest w body ns []
setInputs (inp:inps) (MapNest _ body ns _) = MapNest w body ns' (inp:inps)
  where w = arraySize 0 $ SOAC.inputType inp
        ws = drop 1 $ arrayDims $ SOAC.inputType inp
        ns' = zipWith setDepth ns ws
        setDepth n nw = n { nestingWidth = nw }

fromSOAC :: (Bindable lore, MonadFreshNames m,
             LocalScope lore m,
             Op lore ~ Futhark.SOAC lore) =>
            SOAC lore -> m (Maybe (MapNest lore))
fromSOAC = fromSOAC' mempty

fromSOAC' :: (Bindable lore, MonadFreshNames m,
              LocalScope lore m,
              Op lore ~ Futhark.SOAC lore) =>
             [Ident]
          -> SOAC lore
          -> m (Maybe (MapNest lore))

fromSOAC' bound (SOAC.Screma w (SOAC.ScremaForm (_, []) [] lam) inps) = do
  maybenest <- case (stmsToList $ bodyStms $ lambdaBody lam,
                     bodyResult $ lambdaBody lam) of
    ([Let pat _ e], res) | res == map Var (patternNames pat) ->
      localScope (scopeOfLParams $ lambdaParams lam) $
      SOAC.fromExp e >>=
      either (return . Left) (fmap (Right . fmap (pat,)) . fromSOAC' bound')
    _ ->
      return $ Right Nothing

  case maybenest of
    -- Do we have a nested MapNest?
    Right (Just (pat, mn@(MapNest inner_w body' ns' inps'))) -> do
      (ps, inps'') <-
        unzip <$>
        fixInputs w (zip (map paramName $ lambdaParams lam) inps)
        (zip (params mn) inps')
      let n' = Nesting { nestingParamNames = ps
                       , nestingResult     = patternNames pat
                       , nestingReturnType = typeOf mn
                       , nestingWidth      = inner_w
                       }
      return $ Just $ MapNest w body' (n':ns') inps''
    -- No nested MapNest it seems.
    _ -> do
      let isBound name
            | Just param <- find ((name==) . identName) bound =
              Just param
            | otherwise =
              Nothing
          boundUsedInBody =
            mapMaybe isBound $ S.toList $ freeIn lam
      newParams <- mapM (newIdent' (++"_wasfree")) boundUsedInBody
      let subst = M.fromList $
                  zip (map identName boundUsedInBody) (map identName newParams)
          inps' = inps ++
                  map (SOAC.addTransform (SOAC.Replicate mempty $ Shape [w]) . SOAC.identInput)
                  boundUsedInBody
          lam' =
            lam { lambdaBody =
                    substituteNames subst $ lambdaBody lam
                , lambdaParams =
                    lambdaParams lam ++ [ Param name t
                                        | Ident name t <- newParams ]
                }
      return $ Just $ MapNest w lam' [] inps'
  where bound' = bound <> map paramIdent (lambdaParams lam)

fromSOAC' _ _ = return Nothing

toSOAC :: (MonadFreshNames m, HasScope lore m,
           Bindable lore, BinderOps lore, Op lore ~ Futhark.SOAC lore) =>
          MapNest lore -> m (SOAC lore)
toSOAC (MapNest w lam [] inps) =
  return $ SOAC.Screma w (Futhark.mapSOAC lam) inps
toSOAC (MapNest w lam (Nesting npnames nres nrettype nw:ns) inps) = do
  let nparams = zipWith Param npnames $ map SOAC.inputRowType inps
  (e,bnds) <- runBinder $ localScope (scopeOfLParams nparams) $ SOAC.toExp =<<
    toSOAC (MapNest nw lam ns $ map (SOAC.identInput . paramIdent) nparams)
  bnd <- mkLetNames nres e
  let outerlam = Lambda { lambdaParams = nparams
                        , lambdaBody = mkBody (bnds<>oneStm bnd) $ map Var nres
                        , lambdaReturnType = nrettype
                        }
  return $ SOAC.Screma w (Futhark.mapSOAC outerlam) inps

fixInputs :: MonadFreshNames m =>
             SubExp -> [(VName, SOAC.Input)] -> [(VName, SOAC.Input)]
          -> m [(VName, SOAC.Input)]
fixInputs w ourInps = mapM inspect
  where
    isParam x (y, _) = x == y

    inspect (_, SOAC.Input ts v _)
      | Just (p,pInp) <- find (isParam v) ourInps = do
          let pInp' = SOAC.transformRows ts pInp
          p' <- newNameFromString $ baseString p
          return (p', pInp')

    inspect (param, SOAC.Input ts a t) = do
      param' <- newNameFromString (baseString param ++ "_rep")
      return (param', SOAC.Input (ts SOAC.|> SOAC.Replicate mempty (Shape [w])) a t)
