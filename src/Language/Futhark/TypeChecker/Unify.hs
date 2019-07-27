{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Language.Futhark.TypeChecker.Unify
  ( Constraint(..)
  , Usage
  , mkUsage
  , mkUsage'
  , Constraints
  , lookupSubst
  , MonadUnify(..)
  , Rigidity(..)
  , BreadCrumb(..)
  , typeError
  , mkTypeVarName

  , zeroOrderType
  , mustHaveConstr
  , mustHaveField
  , mustBeOneOf
  , equalityType
  , normaliseType
  , instantiateMissingDims
  , instantiateEmptyArrayDims

  , unify
  , unifyMostCommon
  , anyDimOnMismatch
  , doUnification
  )
where
import Debug.Trace
import Control.Monad.Except
import Control.Monad.State
import Control.Monad.Writer hiding (Sum)
import Data.Bitraversable
import Data.List
import Data.Loc
import Data.Maybe
import qualified Data.Map.Strict as M
import qualified Data.Set as S

import Language.Futhark
import Language.Futhark.TypeChecker.Monad hiding (BoundV)
import Language.Futhark.TypeChecker.Types
import Futhark.Util.Pretty (Pretty)

-- | Mapping from fresh type variables, instantiated from the type
-- schemes of polymorphic functions, to (possibly) specific types as
-- determined on application and the location of that application, or
-- a partial constraint on their type.
type Constraints = M.Map VName Constraint

-- | A usage that caused a type constraint.
data Usage = Usage (Maybe String) SrcLoc

mkUsage :: SrcLoc -> String -> Usage
mkUsage = flip (Usage . Just)

mkUsage' :: SrcLoc -> Usage
mkUsage' = Usage Nothing

instance Show Usage where
  show (Usage Nothing loc) = "use at " ++ locStr loc
  show (Usage (Just s) loc) = s ++ " at " ++ locStr loc

instance Located Usage where
  locOf (Usage _ loc) = locOf loc

data Constraint = NoConstraint (Maybe Liftedness) Usage
                | ParamType Liftedness SrcLoc
                | Constraint StructType Usage
                | Overloaded [PrimType] Usage
                | HasFields (M.Map Name StructType) Usage
                | Equality Usage
                | HasConstrs (M.Map Name [StructType]) Usage
                | Size (Maybe (DimDecl VName)) Usage
                  -- ^ Is not actually a type, but a term-level size,
                  -- possibly already set to something specific.
                | UnknowableSize SrcLoc
                  -- ^ A size that does not unify with anything -
                  -- created from the result of applying a function
                  -- whose return size is existential.
                deriving Show

instance Located Constraint where
  locOf (NoConstraint _ usage) = locOf usage
  locOf (ParamType _ usage) = locOf usage
  locOf (Constraint _ usage) = locOf usage
  locOf (Overloaded _ usage) = locOf usage
  locOf (HasFields _ usage) = locOf usage
  locOf (Equality usage) = locOf usage
  locOf (HasConstrs _ usage) = locOf usage
  locOf (Size _ usage) = locOf usage
  locOf (UnknowableSize usage) = locOf usage

lookupSubst :: VName -> Constraints -> Maybe (Subst StructType)
lookupSubst v constraints = case M.lookup v constraints of
                              Just (Constraint t _) -> Just $ Subst t
                              Just Overloaded{} -> Just PrimSubst
                              Just (Size (Just v2) _) -> Just $ SizeSubst v2
                              _ -> Nothing

-- | The ridigity of a type- or dimension variable.
data Rigidity = Rigid | Nonrigid

class (MonadBreadCrumbs m, MonadError TypeError m) => MonadUnify m where
  getConstraints :: m Constraints
  putConstraints :: Constraints -> m ()
  modifyConstraints :: (Constraints -> Constraints) -> m ()
  modifyConstraints f = do
    x <- getConstraints
    putConstraints $ f x

  newTypeVar :: Monoid als => SrcLoc -> String -> m (TypeBase dim als)
  newDimVar :: SrcLoc -> Rigidity -> String -> m VName

normaliseType :: (Substitutable a, MonadUnify m) => a -> m a
normaliseType t = do constraints <- getConstraints
                     return $ applySubst (`lookupSubst` constraints) t

data Position = Positive | Negative

traverseDims :: forall m als1 . Monad m =>
                (Position -> DimDecl VName -> m (DimDecl VName))
             -> TypeBase (DimDecl VName) als1
             -> m (TypeBase (DimDecl VName) als1)
traverseDims onDim (Scalar (Arrow als p t1 t2)) =
  Scalar <$> (Arrow als p <$> onParam t1 <*> traverseDims onDim t2)
  where onParam :: forall als2 .
                   TypeBase (DimDecl VName) als2
                -> m (TypeBase (DimDecl VName) als2)
        onParam t@(Scalar Arrow{}) = bitraverse (onDim Negative) pure t
        onParam t@Array{} = bitraverse (onDim Positive) pure t
        onParam (Scalar (Record fs)) =
          Scalar . Record <$> traverse onParam fs
        onParam (Scalar (TypeVar as u tn targs)) =
          Scalar . TypeVar as u tn <$> mapM onTypeArg targs
        onParam (Scalar (Sum cs)) =
          Scalar . Sum <$> traverse (mapM onParam) cs
        onParam (Scalar (Prim t)) = return $ Scalar $ Prim t
        onTypeArg (TypeArgDim d loc) =
          TypeArgDim <$> onDim Positive d <*> pure loc
        onTypeArg (TypeArgType t loc) =
          TypeArgType <$> onParam t <*> pure loc
traverseDims onDim (Scalar (Record fs)) =
  Scalar . Record <$> traverse (traverseDims onDim) fs
traverseDims onDim t = bitraverse (onDim Negative) pure t

-- | Synthesize nonrigid dimension names for 'AnyDim'
-- dimensions in positive position in the given type.
instantiateMissingDims :: MonadUnify m =>
                          SrcLoc
                       -> TypeBase (DimDecl VName) als
                       -> m (TypeBase (DimDecl VName) als, [VName])
instantiateMissingDims loc = runWriterT . traverseDims onDim
  where onDim Positive AnyDim = do
          dim <- lift $ newDimVar loc Nonrigid "dim"
          tell [dim]
          return $ NamedDim $ qualName dim
        onDim _ d = return d

instantiateEmptyArrayDims :: MonadUnify f =>
                             SrcLoc -> Rigidity -> TypeBase (DimDecl VName) als
                          -> f (TypeBase (DimDecl VName) als, [VName])
instantiateEmptyArrayDims loc r = runWriterT . instantiate
  where instantiate t@Array{} = bitraverse onDim pure t
        instantiate (Scalar (Record fs)) = Scalar . Record <$> traverse instantiate fs
        instantiate t = return t

        onDim AnyDim = do v <- lift $ newDimVar loc r "impl_dim"
                          tell [v]
                          return $ NamedDim $ qualName v
        onDim d = pure d

-- | Is the given type variable actually the name of an abstract type
-- or type parameter, which we cannot substitute?
isRigid :: VName -> Constraints -> Bool
isRigid v constraints = case M.lookup v constraints of
                          Nothing -> True
                          Just ParamType{} -> True
                          Just UnknowableSize{} -> True
                          _ -> False

-- | Unifies two types.
unify :: MonadUnify m => Usage -> StructType -> StructType -> m ()
unify usage orig_t1 orig_t2 = do
  orig_t1' <- normaliseType orig_t1
  orig_t2' <- normaliseType orig_t2
  breadCrumb (MatchingTypes orig_t1' orig_t2') $ subunify orig_t1 orig_t2
  where
    subunify t1 t2 = do
      constraints <- getConstraints

      let isRigid' v = isRigid v constraints
          t1' = applySubst (`lookupSubst` constraints) t1
          t2' = applySubst (`lookupSubst` constraints) t2

          failure =
            typeError usage $ "Couldn't match expected type `" ++
            pretty t1' ++ "' with actual type `" ++ pretty t2' ++ "'."

          unifyDims' d1 d2
            | isJust $ unifyDims d1 d2 = return ()
          unifyDims' (NamedDim (QualName _ d1)) d2
            | not $ isRigid' d1 =
                linkVarToDim usage d1 d2
          unifyDims' d1 (NamedDim (QualName _ d2))
            | not $ isRigid' d2 =
                linkVarToDim usage d2 d1
          unifyDims' d1 d2 =
            typeError usage $ "Dimensions " ++ quote (pretty d1) ++
            " and " ++ quote (pretty d2) ++ " do not match."

      case (t1', t2') of
        _ | t1' == t2' -> return ()

        (Scalar (Record fs),
         Scalar (Record arg_fs))
          | M.keys fs == M.keys arg_fs ->
              forM_ (M.toList $ M.intersectionWith (,) fs arg_fs) $ \(k, (k_t1, k_t2)) ->
              breadCrumb (MatchingFields k) $ subunify k_t1 k_t2

        (Scalar (TypeVar _ _ (TypeName _ tn) targs),
         Scalar (TypeVar _ _ (TypeName _ arg_tn) arg_targs))
          | tn == arg_tn, length targs == length arg_targs ->
              zipWithM_ unifyTypeArg targs arg_targs

        (Scalar (TypeVar _ _ (TypeName [] v1) []),
         Scalar (TypeVar _ _ (TypeName [] v2) [])) ->
          case (isRigid' v1, isRigid' v2) of
            (True, True) -> failure
            (True, False) -> linkVarToType usage v2 t1'
            (False, True) -> linkVarToType usage v1 t2'
            (False, False) -> linkVarToType usage v1 t2'

        (Scalar (TypeVar _ _ (TypeName [] v1) []), _)
          | not $ isRigid' v1 ->
              linkVarToType usage v1 t2'
        (_, Scalar (TypeVar _ _ (TypeName [] v2) []))
          | not $ isRigid' v2 ->
              linkVarToType usage v2 t1'

        (Scalar (Arrow _ p1 a1 b1),
         Scalar (Arrow _ p2 a2 b2)) -> do
          subunify a1 a2
          subunify b1' b2'
          where (b1', b2') =
                  -- Replace one parameter name with the other in the
                  -- return type, in case of dependent types.  I.e.,
                  -- we want type '(n: i32) -> [n]i32' to unify with
                  -- type '(x: i32) -> [x]i32'.
                  --
                  -- In case only one of the functions has a named
                  -- parameter, we weaken that type by removing all
                  -- references to the parameter.  XXX: is this
                  -- sensible?
                  case (p1, p2) of
                    (Just p1', Just p2') ->
                      let f v | v == p2' = Just $ SizeSubst $ NamedDim $ qualName p1'
                              | otherwise = Nothing
                      in (b1, applySubst f b2)
                    _ ->
                      (b1, b2)

        (Array{}, Array{})
          | ShapeDecl (t1_d : _) <- arrayShape t1',
            ShapeDecl (t2_d : _) <- arrayShape t2',
            Just t1'' <- peelArray 1 t1',
            Just t2'' <- peelArray 1 t2' -> do
              unifyDims' t1_d t2_d
              subunify t1'' t2''

        (Scalar (Sum cs),
         Scalar (Sum arg_cs))
          | M.keys cs == M.keys arg_cs ->
              forM_ (M.toList $ M.intersectionWith (,) cs arg_cs) $ \(_, (f1, f2)) ->
              if length f1 == length f2
              then zipWithM_ subunify f1 f2 -- TODO: improve
              else failure
        (_, _) -> failure

      where unifyTypeArg TypeArgDim{} TypeArgDim{} = return ()
            unifyTypeArg (TypeArgType t _) (TypeArgType arg_t _) =
              subunify t arg_t
            unifyTypeArg _ _ = typeError usage
              "Cannot unify a type argument with a dimension argument (or vice versa)."

applySubstInConstraint :: VName -> Subst StructType -> Constraint -> Constraint
applySubstInConstraint vn subst (Constraint t loc) =
  Constraint (applySubst (flip M.lookup $ M.singleton vn subst) t) loc
applySubstInConstraint vn subst (HasFields fs loc) =
  HasFields (M.map (applySubst (flip M.lookup $ M.singleton vn subst)) fs) loc
applySubstInConstraint _ _ (NoConstraint l loc) = NoConstraint l loc
applySubstInConstraint _ _ (Overloaded ts usage) = Overloaded ts usage
applySubstInConstraint _ _ (Equality loc) = Equality loc
applySubstInConstraint _ _ (ParamType l loc) = ParamType l loc
applySubstInConstraint vn subst (HasConstrs cs loc) =
  HasConstrs (M.map (map (applySubst (flip M.lookup $ M.singleton vn subst))) cs) loc
applySubstInConstraint vn (SizeSubst v') (Size (Just (NamedDim v)) loc)
  | vn == qualLeaf v = Size (Just v') loc
applySubstInConstraint _ _ (Size v loc) = Size v loc
applySubstInConstraint _ _ (UnknowableSize loc) = UnknowableSize loc

linkVarToType :: MonadUnify m => Usage -> VName -> StructType -> m ()
linkVarToType usage vn tp = do
  constraints <- getConstraints
  if vn `S.member` typeVars tp
    then typeError usage $ "Occurs check: cannot instantiate " ++
         prettyName vn ++ " with " ++ pretty tp'
    else do modifyConstraints $ M.insert vn $ Constraint tp' usage
            modifyConstraints $ M.map $ applySubstInConstraint vn $ Subst tp'
            case M.lookup vn constraints of

              Just (NoConstraint (Just Unlifted) unlift_usage) ->
                zeroOrderType usage (show unlift_usage) tp'

              Just (Equality _) ->
                equalityType usage tp'

              Just (Overloaded ts old_usage)
                | tp `notElem` map (Scalar . Prim) ts ->
                    case tp' of
                      Scalar (TypeVar _ _ (TypeName [] v) [])
                        | not $ isRigid v constraints -> linkVarToTypes usage v ts
                      _ ->
                        typeError usage $ "Cannot unify `" ++ prettyName vn ++ "' with type `" ++
                          pretty tp ++ "' (`" ++ prettyName vn ++
                          "` must be one of " ++ intercalate ", " (map pretty ts) ++
                          " due to " ++ show old_usage ++ ")."

              Just (HasFields required_fields old_usage) ->
                case tp of
                  Scalar (Record tp_fields)
                    | all (`M.member` tp_fields) $ M.keys required_fields ->
                        mapM_ (uncurry $ unify usage) $ M.elems $
                        M.intersectionWith (,) required_fields tp_fields
                  Scalar (TypeVar _ _ (TypeName [] v) [])
                    | not $ isRigid v constraints ->
                        modifyConstraints $ M.insert v $
                        HasFields required_fields old_usage
                  _ ->
                    let required_fields' =
                          intercalate ", " $ map field $ M.toList required_fields
                        field (l, t) = pretty l ++ ": " ++ pretty t
                    in typeError usage $
                       "Cannot unify `" ++ prettyName vn ++ "' with type `" ++
                       pretty tp ++ "' (must be a record with fields {" ++
                       required_fields' ++
                       "} due to " ++ show old_usage ++ ")."

              Just (HasConstrs required_cs old_usage) ->
                case tp of
                  Scalar (Sum ts)
                    | all (`M.member` ts) $ M.keys required_cs ->
                        mapM_ (uncurry (zipWithM_ (unify usage))) $ M.elems $
                          M.intersectionWith (,) required_cs ts
                  Scalar (TypeVar _ _ (TypeName [] v) [])
                    | not $ isRigid v constraints ->
                        modifyConstraints $ M.insertWith combineConstrs v $
                        HasConstrs required_cs old_usage
                        where combineConstrs (HasConstrs cs1 usage1) (HasConstrs cs2 _) =
                                HasConstrs (M.union cs1 cs2) usage1
                              combineConstrs hasCs _ = hasCs
                  _ -> typeError usage "Cannot unify a sum type with a non-sum type"
              _ -> return ()
  where tp' = removeUniqueness tp

linkVarToDim :: MonadUnify m => Usage -> VName -> DimDecl VName -> m ()
linkVarToDim usage vn1 dim = do
  modifyConstraints $ M.insert vn1 $ Size (Just dim) usage
  modifyConstraints $ M.map $ applySubstInConstraint vn1 $ SizeSubst dim

removeUniqueness :: TypeBase dim as -> TypeBase dim as
removeUniqueness (Scalar (Record ets)) =
  Scalar $ Record $ fmap removeUniqueness ets
removeUniqueness (Scalar (Arrow als p t1 t2)) =
  Scalar $ Arrow als p (removeUniqueness t1) (removeUniqueness t2)
removeUniqueness (Scalar (Sum cs)) =
  Scalar $ Sum $ (fmap . fmap) removeUniqueness cs
removeUniqueness t = t `setUniqueness` Nonunique

mustBeOneOf :: MonadUnify m => [PrimType] -> Usage -> StructType -> m ()
mustBeOneOf [req_t] usage t = unify usage (Scalar (Prim req_t)) t
mustBeOneOf ts usage t = do
  constraints <- getConstraints
  let t' = applySubst (`lookupSubst` constraints) t
      isRigid' v = isRigid v constraints

  case t' of
    Scalar (TypeVar _ _ (TypeName [] v) [])
      | not $ isRigid' v -> linkVarToTypes usage v ts

    Scalar (Prim pt) | pt `elem` ts -> return ()

    _ -> failure

  where failure = typeError usage $ "Cannot unify type \"" ++ pretty t ++
                  "\" with any of " ++ intercalate "," (map pretty ts) ++ "."

linkVarToTypes :: MonadUnify m => Usage -> VName -> [PrimType] -> m ()
linkVarToTypes usage vn ts = do
  vn_constraint <- M.lookup vn <$> getConstraints
  case vn_constraint of
    Just (Overloaded vn_ts vn_usage) ->
      case ts `intersect` vn_ts of
        [] -> typeError usage $ "Type constrained to one of " ++
              intercalate "," (map pretty ts) ++ " but also one of " ++
              intercalate "," (map pretty vn_ts) ++ " due to " ++ show vn_usage ++ "."
        ts' -> modifyConstraints $ M.insert vn $ Overloaded ts' usage

    _ -> modifyConstraints $ M.insert vn $ Overloaded ts usage

equalityType :: (MonadUnify m, Pretty (ShapeDecl dim), Monoid as) =>
                Usage -> TypeBase dim as -> m ()
equalityType usage t = do
  unless (orderZero t) $
    typeError usage $
    "Type \"" ++ pretty t ++ "\" does not support equality (is higher-order)."
  mapM_ mustBeEquality $ typeVars t
  where mustBeEquality vn = do
          constraints <- getConstraints
          case M.lookup vn constraints of
            Just (Constraint (Scalar (TypeVar _ _ (TypeName [] vn') [])) _) ->
              mustBeEquality vn'
            Just (Constraint vn_t cusage)
              | not $ orderZero vn_t ->
                  typeError usage $
                  unlines ["Type \"" ++ pretty t ++ "\" does not support equality.",
                           "Constrained to be higher-order due to " ++ show cusage ++ "."]
              | otherwise -> return ()
            Just (NoConstraint _ _) ->
              modifyConstraints $ M.insert vn (Equality usage)
            Just (Overloaded _ _) ->
              return () -- All primtypes support equality.
            Just (HasConstrs cs _) ->
              mapM_ (equalityType usage) $ concat $ M.elems cs
            _ ->
              typeError usage $ "Type " ++ pretty (prettyName vn) ++
              " does not support equality."

zeroOrderType :: (MonadUnify m, Pretty (ShapeDecl dim), Monoid as) =>
                 Usage -> String -> TypeBase dim as -> m ()
zeroOrderType usage desc t = do
  unless (orderZero t) $
    typeError usage $ "Type " ++ desc ++
    " must not be functional, but is " ++ quote (pretty t) ++ "."
  mapM_ mustBeZeroOrder . S.toList . typeVars $ t
  where mustBeZeroOrder vn = do
          constraints <- getConstraints
          case M.lookup vn constraints of
            Just (Constraint vn_t old_usage)
              | not $ orderZero t ->
                typeError usage $ "Type " ++ desc ++
                " must be non-function, but inferred to be " ++
                quote (pretty vn_t) ++ " due to " ++ show old_usage ++ "."
            Just (NoConstraint _ _) ->
              modifyConstraints $ M.insert vn (NoConstraint (Just Unlifted) usage)
            Just (ParamType Lifted ploc) ->
              typeError usage $ "Type " ++ desc ++
              " must be non-function, but type parameter " ++ quote (prettyName vn) ++ " at " ++
              locStr ploc ++ " may be a function."
            _ -> return ()

-- In @mustHaveConstr usage c t fs@, the type @t@ must have a
-- constructor named @c@ that takes arguments of types @ts@.
mustHaveConstr :: MonadUnify m =>
                  Usage -> Name -> StructType -> [StructType] -> m ()
mustHaveConstr usage c t fs = do
  constraints <- getConstraints
  case t of
    Scalar (TypeVar _ _ (TypeName _ tn) [])
      | Just NoConstraint{} <- M.lookup tn constraints ->
          modifyConstraints $ M.insert tn $ HasConstrs (M.singleton c fs) usage
      | Just (HasConstrs cs _) <- M.lookup tn constraints ->
        case M.lookup c cs of
          Nothing  -> modifyConstraints $ M.insert tn $ HasConstrs (M.insert c fs cs) usage
          Just fs'
            | length fs == length fs' -> zipWithM_ (unify usage) fs fs'
            | otherwise -> typeError usage $ "Different arity for constructor "
                           ++ quote (pretty c) ++ "."

    Scalar (Sum cs) ->
      case M.lookup c cs of
        Nothing -> typeError usage $ "Constuctor " ++ quote (pretty c) ++ " not present in type."
        Just fs'
            | length fs == length fs' -> zipWithM_ (unify usage) fs fs'
            | otherwise -> typeError usage $ "Different arity for constructor " ++
                           quote (pretty c) ++ "."

    _ -> do unify usage t $ Scalar $ Sum $ M.singleton c fs
            return ()

mustHaveField :: MonadUnify m =>
                 Usage -> Name -> PatternType -> m PatternType
mustHaveField usage l t = do
  constraints <- getConstraints
  l_type <- newTypeVar (srclocOf usage) "t"
  let l_type' = toStruct l_type
  case t of
    Scalar (TypeVar _ _ (TypeName _ tn) [])
      | Just NoConstraint{} <- M.lookup tn constraints -> do
          modifyConstraints $ M.insert tn $ HasFields (M.singleton l l_type') usage
          return l_type
      | Just (HasFields fields _) <- M.lookup tn constraints -> do
          case M.lookup l fields of
            Just t' -> unify usage l_type' t'
            Nothing -> modifyConstraints $ M.insert tn $
                       HasFields (M.insert l l_type' fields) usage
          return l_type
    Scalar (Record fields)
      | Just t' <- M.lookup l fields -> do
          unify usage l_type' $ toStruct t'
          return t'
      | otherwise ->
          typeError usage $
          "Attempt to access field '" ++ pretty l ++ "' of value of type " ++
          pretty (toStructural t) ++ "."
    _ -> do unify usage (toStruct t) $ Scalar $ Record $ M.singleton l l_type'
            return l_type

matchDims :: (Monoid as, Monad m) =>
             (d -> d -> m d)
          -> TypeBase d as -> TypeBase d as
          -> m (TypeBase d as)
matchDims onDims t1 t2 =
  case (t1, t2) of
    (Array als1 u1 et1 shape1, Array als2 u2 et2 shape2) ->
      flip setAliases (als1<>als2) <$>
      (arrayOf <$>
       matchDims onDims (Scalar et1) (Scalar et2) <*>
       onShapes shape1 shape2 <*> pure (min u1 u2))
    (Scalar (Record f1), Scalar (Record f2)) ->
      Scalar . Record <$> traverse (uncurry (matchDims onDims)) (M.intersectionWith (,) f1 f2)
    (Scalar (TypeVar als1 u v targs1),
     Scalar (TypeVar als2 _ _ targs2)) ->
      Scalar . TypeVar (als1 <> als2) u v <$> zipWithM matchTypeArg targs1 targs2
    _ -> return t1

  where matchTypeArg ta@TypeArgType{} _ = return ta
        matchTypeArg a _ = return a

        onShapes shape1 shape2 =
          ShapeDecl <$> zipWithM onDims (shapeDims shape1) (shapeDims shape2)

-- | Replace dimension mismatches with AnyDim.  Where one of the types
-- contains an AnyDim dimension, the corresponding dimension in the
-- other type is used.
anyDimOnMismatch :: Monoid as =>
                    TypeBase (DimDecl VName) as -> TypeBase (DimDecl VName) as
                 -> (TypeBase (DimDecl VName) as, [(DimDecl VName, DimDecl VName)])
anyDimOnMismatch t1 t2 = runWriter $ matchDims onDims t1 t2
  where onDims AnyDim d2 = return d2
        onDims d1 AnyDim = return d1
        onDims d1 d2
          | d1 == d2 = return d1
          | otherwise = do tell [(d1, d2)]
                           return AnyDim

-- | Like unification, but creates new size variables where mismatches
-- occur.  Returns the new dimensions thus created.
unifyMostCommon :: MonadUnify m =>
                   Usage -> PatternType -> PatternType -> m (PatternType, [VName])
unifyMostCommon usage t1 t2 = do
  -- We are ignoring the dimensions here, because any mismatches
  -- should be turned into fresh size variables.
  unify usage (toStruct (anyDimShapeAnnotations t1))
              (toStruct (anyDimShapeAnnotations t2))
  t1' <- normaliseType t1
  t2' <- normaliseType t2
  instantiateEmptyArrayDims (srclocOf usage) Rigid $ fst $ anyDimOnMismatch t1' t2'

-- Simple MonadUnify implementation.

type UnifyMState = (Constraints, Int)

newtype UnifyM a = UnifyM (StateT UnifyMState (Except TypeError) a)
  deriving (Monad, Functor, Applicative,
            MonadState UnifyMState,
            MonadError TypeError)

newVar :: String -> UnifyM VName
newVar name = do
  (x, i) <- get
  put (x, i+1)
  return $ VName (mkTypeVarName name i) i

instance MonadUnify UnifyM where
  getConstraints = gets fst
  putConstraints x = modify $ \s -> (x, snd s)

  newTypeVar loc name = do
    v <- newVar name
    modifyConstraints $ M.insert v $ NoConstraint Nothing $ Usage Nothing loc
    return $ Scalar $ TypeVar mempty Nonunique (typeName v) []

  newDimVar loc rigidity name = do
    dim <- newVar name
    case rigidity of
      Rigid -> modifyConstraints $ M.insert dim $ UnknowableSize loc
      Nonrigid -> modifyConstraints $ M.insert dim $ Size Nothing $ Usage Nothing loc
    return dim

-- | Construct a the name of a new type variable given a base
-- description and a tag number (note that this is distinct from
-- actually constructing a VName; the tag here is intended for human
-- consumption but the machine does not care).
mkTypeVarName :: String -> Int -> Name
mkTypeVarName desc i =
  nameFromString $ desc ++ mapMaybe subscript (show i)
  where subscript = flip lookup $ zip "0123456789" "₀₁₂₃₄₅₆₇₈₉"

instance MonadBreadCrumbs UnifyM where

runUnifyM :: [TypeParam] -> UnifyM a -> Either TypeError a
runUnifyM tparams (UnifyM m) = runExcept $ evalStateT m (constraints, 0)
  where constraints = M.fromList $ map f tparams
        f (TypeParamDim p loc) = (p, Size Nothing $ Usage Nothing loc)
        f (TypeParamType l p loc) = (p, NoConstraint (Just l) $ Usage Nothing loc)

-- | Perform a unification of two types outside a monadic context.
-- The type parameters are allowed to be instantiated; all other types
-- are considered rigid.
doUnification :: SrcLoc -> [TypeParam]
              -> Rigidity -> StructType -> Rigidity -> StructType
              -> Either TypeError StructType
doUnification loc tparams r1 t1 r2 t2 = runUnifyM tparams $ do
  (t1', _) <- instantiateEmptyArrayDims loc r1 t1
  (t2', _) <- instantiateEmptyArrayDims loc r2 t2
  unify (Usage Nothing loc) t1' t2'
  normaliseType t2
