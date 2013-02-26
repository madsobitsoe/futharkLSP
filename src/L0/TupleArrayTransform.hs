module L0.TupleArrayTransform
  ( transformProg
  , transformType )
  where

import Control.Applicative
import Control.Monad.State

import Data.Array
import Data.Data
import Data.Generics
import Data.List
import Data.Loc

import L0.AbSyn
import L0.FreshNames

transformProg :: Prog Type -> Prog Type
transformProg = map transformFun

data TransformState = TransformState {
    envNameSrc :: NameSource
  }

type TransformM = State TransformState

new :: String -> TransformM String
new k = do (name, src) <- gets $ newName k . envNameSrc
           modify $ \s -> s { envNameSrc = src }
           return name

transformType :: Type -> Type
transformType (Array (Tuple elemts _) size loc) =
  Tuple (map (transformType . arraytype) elemts) loc
  where arraytype t = Array t size loc
transformType (Array elemt size loc) =
  case elemt' of
    Tuple elemts _ -> Tuple (map (transformType . arraytype) elemts) loc
    _ -> Array elemt' size loc
  where elemt' = transformType elemt
        arraytype t = transformType $ Array t size loc
transformType (Tuple elemts loc) = Tuple (map transformType elemts) loc
transformType t = t -- All other types are fine.

transformValue :: Value -> Value
transformValue (ArrayVal arr et loc) =
  case transformType et of
    Tuple ets _ -> TupVal (zipWith asarray ets $ transpose arrayvalues) loc
    et'         -> ArrayVal arr et' loc
  where asarray t vs = transformValue $ arrayVal vs t loc
        arrayvalues = map (tupleValues . transformValue) $ elems arr
        tupleValues (TupVal vs _) = vs
        tupleValues _ = error "L0.TupleArrayTransform.transformValue: Element of tuple array is not tuple."
        -- Above should never happen in well-typed program.
transformValue (TupVal vs loc) = TupVal (map transformValue vs) loc
transformValue v = v

transformPat :: TupIdent Type -> TupIdent Type
transformPat (TupId pats loc) = TupId (map transformPat pats) loc
transformPat (Id k) = Id $ transformIdent k

transformIdent :: Ident Type -> Ident Type
transformIdent (Ident name t loc) = Ident name (transformType t) loc

transformFun :: FunDec Type -> FunDec Type
transformFun (fname,rettype,params,body,loc) = (fname, rettype', params', body', loc)
  where rettype' = transformType rettype
        params' = map transformIdent params
        body' = runTransformM $ transformExp body
        runTransformM = flip evalState newState
        newState = TransformState $ newNameSource []

transformExp :: Exp Type -> TransformM (Exp Type)
transformExp (Literal val) =
  return $ Literal $ transformValue val
transformExp (Var k) =
  return $ Var $ transformIdent k
transformExp (TupLit es loc) = do
  es' <- mapM transformExp es
  return $ TupLit es' loc
transformExp (ArrayLit es intype loc) = do
  es' <- mapM transformExp es
  case transformType intype of
    Tuple ets _ -> do
      (e, bindings) <- foldM comb (id, replicate (length ets) []) es'
      e <$> tuparrexp (map reverse bindings) ets
        where comb (acce, bindings) e = do
                names <- map fst <$> mapM (newVar "array_tup") ets
                return (\body -> acce $ LetPat (TupId (map Id names) loc) e body loc
                       ,zipWith (:) names bindings)
              tuparrexp bindings ts = transformExp $ TupLit (zipWith arrexp ts bindings) loc
              arrexp t names = ArrayLit (map Var names) t loc
    et' -> return $ ArrayLit es' et' loc
transformExp (Index vname idxs intype outtype loc) = do
  idxs' <- mapM transformExp idxs
  case (identType vname', intype', outtype') of
    -- If the input type is a tuple, then the output type is
    -- necessarily also.
    (Tuple ets _, Tuple its _, Tuple ots _) -> do
      -- Create names for the elements of the tuple.
      names <- map fst <$> mapM (newVar "index_tup") ets
      let indexing = TupLit (zipWith (\name (it,ot) -> Index name idxs' it ot loc) names (zip its ots)) loc
      return $ LetPat (TupId (map Id names) loc) (Var vname') indexing loc
    _ -> return $ Index vname' idxs' intype' outtype' loc
  where intype' = transformType intype
        outtype' = transformType outtype
        vname' = transformIdent vname
transformExp (DoLoop i bound body mergevars loc) = do
  bound' <- transformExp bound
  body' <- transformExp body
  return $ DoLoop (transformIdent i) bound' body' (map transformIdent mergevars) loc
transformExp (Replicate ne ve ty loc) = do
  ne' <- transformExp ne
  ve' <- transformExp ve
  case ty' of
    Tuple ets _ -> do
      (n, nv) <- newVar "n" (Int loc)
      (names, vs) <- unzip <$> mapM (newVar "rep_tuple") ets
      let arrexp v = Replicate nv v (expType v) loc
          nlet body = LetPat (Id n) ne' body loc
          tuplet body = LetPat (TupId (map Id names) loc) ve' body loc
      return $ nlet $ tuplet $ TupLit (map arrexp vs) loc
    _ -> return $ Replicate ne' ve' ty' loc
  where ty' = transformType ty
transformExp (LetWith name e idxs ve body loc) = do
  e' <- transformExp e
  idxs' <- mapM transformExp idxs
  body' <- transformExp body
  ve' <- transformExp ve
  case (identType name', expType ve') of
    (Tuple ets _, Tuple xts _) -> do
      snames <- map fst <$> mapM (newVar "letwith_src") ets
      vnames <- map fst <$> mapM (newVar "letwith_el") xts
      let xlet inner = LetPat (TupId (map Id snames) loc) (Copy e' loc) inner loc
          vlet inner = LetPat (TupId (map Id vnames) loc) ve' inner loc
          comb olde (sname, vname) inner = LetWith sname (Var sname) idxs' (Var vname) (olde inner) loc
      let lws = foldl comb id $ zip snames vnames
      return $ xlet $ vlet $ lws $ LetWith name' (Var name') [] (TupLit (map Var snames) loc) body' loc
    _ -> return $ LetWith name' e' idxs' ve' body' loc
  where name' = transformIdent name
transformExp (Size e loc) = do
  e' <- transformExp e
  case expType e' of
    Tuple (et:ets) _ -> do
      (name, namev) <- newVar "size_tup" et
      names <- map fst <$> mapM (newVar "size_tup") ets
      return $ LetPat (TupId (map Id $ name : names) loc) e' (Size namev loc) loc
    _ -> return $ Size e' loc
transformExp (Unzip e _ _) = transformExp e
transformExp (Zip ((e,t):es) loc) = do
  -- Zip is not quite an identity, as we need to truncate the arrays
  -- to the length of the shortest one.
  e' <- transformExp e
  es' <- mapM (transformExp . fst) es
  let ets = map transformType $ t : map snd es
  (name, namev) <- newVar "zip_array" (expType e')
  names <- map fst <$> mapM (newVar "zip_array") (map expType es')
  (size, sizev) <- newVar "zip_size" $ Int loc
  let combes olde (arre,vname) inner = LetPat (Id vname) arre (olde inner) loc
      lete = foldl combes id $ reverse $ zip (e':es') (name:names)
      test vname = BinOp Less sizev (Size (Var vname) loc) (Bool loc) loc
      branch vname = If (test vname) sizev (Size (Var vname) loc) (Int loc) loc
      combsiz olde vname inner = olde $ LetPat (Id size) (branch vname) inner loc
      letsizs = foldl combsiz (\inner -> LetPat (Id size) (Size namev loc) inner loc) names
      split et vname = do
        (a, av) <- newVar "zip_a" $ identType vname
        (b, _) <- newVar "zip_b" $ identType vname
        return $ LetPat (TupId [Id a, Id b] loc) (Split sizev (Var vname) et loc) av loc
  lete . letsizs . (\as -> TupLit as loc) <$> zipWithM split ets (name:names)
transformExp e = gmapM (mkM transformExp
                       `extM` (return . transformType)
                       `extM` mapM transformExp
                       `extM` transformLambda
                       `extM` (return . transformPat)
                       `extM` mapM transformExpPair) e

transformLambda :: Lambda Type -> TransformM (Lambda Type)
transformLambda (AnonymFun params body rettype loc) = do
  body' <- transformExp body
  return $ AnonymFun params' body' rettype' loc
  where rettype' = transformType rettype
        params' = map transformIdent params
transformLambda (CurryFun fname curryargs rettype loc) = do
  curryargs' <- mapM transformExp curryargs
  return $ CurryFun fname curryargs' (transformType rettype) loc

transformExpPair :: (Exp Type, Type) -> TransformM (Exp Type, Type)
transformExpPair (e,t) = do e' <- transformExp e
                            return (e',transformType t)

newVar :: String -> Type -> TransformM (Ident Type, Exp Type)
newVar name tp = do
  x <- new name
  return (Ident x tp loc, Var $ Ident x tp loc)
  where loc = srclocOf tp

testExp :: Exp Type
testExp = Replicate (Literal $ IntVal 10 noLoc) (New (expType arrlit) noLoc) (expType (New (expType arrlit) noLoc)) noLoc -- LetPat (Id arrident) arrlit idx noLoc
  where idx = Index arrident [Literal $ IntVal 0 noLoc] tuptype tuptype noLoc
        arrident = Ident "a" (expType arrlit) noLoc
        arrlit = ArrayLit [tuple 1 '1', tuple 2 '2'] tuptype noLoc
        tuple i c = Literal $ TupVal [IntVal i noLoc, CharVal c noLoc] noLoc
        tuptype = Tuple [Int noLoc, Char noLoc] noLoc
