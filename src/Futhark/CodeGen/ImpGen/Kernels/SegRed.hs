{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
-- | We generate code for non-segmented/single-segment SegRed using
-- the basic approach outlined in the paper "Design and GPGPU
-- Performance of Futhark’s Redomap Construct" (ARRAY '16).  The main
-- deviations are:
--
-- * While we still use two-phase reduction, we use only a single
--   kernel, with the final workgroup to write a result (tracked via
--   an atomic counter) performing the final reduction as well.
--
-- * Instead of depending on storage layout transformations to handle
--   non-commutative reductions efficiently, we slide a
--   'groupsize'-sized window over the input, and perform a parallel
--   reduction for each window.  This sacrifices the notion of
--   efficient sequentialisation, but is sometimes faster and
--   definitely simpler and more predictable (and uses less auxiliary
--   storage).
--
-- For segmented reductions we use the approach from "Strategies for
-- Regular Segmented Reductions on GPU" (FHPC '17).  This involves
-- having two different strategies, and dynamically deciding which one
-- to use based on the number of segments and segment size. We use the
-- (static) @group_size@ to decide which of the following two
-- strategies to choose:
--
-- * Large: uses one or more groups to process a single segment. If
--   multiple groups are used per segment, the intermediate reduction
--   results must be recursively reduced, until there is only a single
--   value per segment.
--
--   Each thread /can/ read multiple elements, which will greatly
--   increase performance; however, if the reduction is
--   non-commutative we will have to use a less efficient traversal
--   (with interim group-wide reductions) to enable coalesced memory
--   accesses, just as in the non-segmented case.
--
-- * Small: is used to let each group process *multiple* segments
--   within a group. We will only use this approach when we can
--   process at least two segments within a single group. In those
--   cases, we would allocate a /whole/ group per segment with the
--   large strategy, but at most 50% of the threads in the group would
--   have any element to read, which becomes highly inefficient.
module Futhark.CodeGen.ImpGen.Kernels.SegRed
  ( compileSegRed
  , compileSegRed'
  )
  where

import Control.Monad.Except
import Data.Maybe
import Data.List

import Prelude hiding (quot, rem)

import Futhark.MonadFreshNames
import Futhark.Transform.Rename
import Futhark.Representation.ExplicitMemory
import qualified Futhark.CodeGen.ImpCode.Kernels as Imp
import Futhark.CodeGen.ImpGen
import Futhark.CodeGen.ImpGen.Kernels.Base
import qualified Futhark.Representation.ExplicitMemory.IndexFunction as IxFun
import Futhark.Util (chunks)
import Futhark.Util.IntegralExp (quotRoundingUp, quot, rem)

type DoSegBody = (KernelConstants -> ([(SubExp, [Imp.Exp])] -> InKernelGen ()) -> InKernelGen ())

-- | Compile 'SegRed' instance to host-level code with calls to
-- various kernels.
compileSegRed :: Pattern ExplicitMemory
              -> KernelSpace
              -> [SegRedOp InKernel]
              -> KernelBody InKernel
              -> CallKernelGen ()
compileSegRed pat space reds body =
  compileSegRed' pat space reds $ \constants red_cont ->
  compileKernelStms constants (kernelBodyStms body) $ do
  let (red_res, map_res) = splitAt (segRedResults reds) $ kernelBodyResult body

  sComment "save map-out results" $ do
    let map_arrs = drop (segRedResults reds) $ patternElements pat
    zipWithM_ (compileKernelResult constants) map_arrs map_res

  red_cont $ zip (map kernelResultSubExp red_res) $ repeat []

-- | Like 'compileSegRed', but where the body is a monadic action.
compileSegRed' :: Pattern ExplicitMemory
               -> KernelSpace
               -> [SegRedOp InKernel]
               -> DoSegBody
               -> CallKernelGen ()
compileSegRed' pat space reds body
  | [(_, Constant (IntValue (Int32Value 1))), _] <- spaceDimensions space =
      nonsegmentedReduction pat space reds body
  | otherwise = do
      segment_size <-
        toExp $ last $ map snd $ spaceDimensions space
      group_size <- toExp $ spaceGroupSize space
      let use_small_segments = segment_size * 2 .<. group_size
      sIf use_small_segments
        (smallSegmentsReduction pat space reds body)
        (largeSegmentsReduction pat space reds body)

-- | Prepare intermediate arrays for the reduction.  Prim-typed
-- arguments go in local memory (so we need to do the allocation of
-- those arrays inside the kernel), while array-typed arguments go in
-- global memory.  Allocations for the former have already been
-- performed.  This policy is baked into how the allocations are done
-- in ExplicitAllocations.
intermediateArrays :: KernelSpace -> SegRedOp InKernel -> InKernelGen [VName]
intermediateArrays space (SegRedOp _ red_op nes _) = do
  let red_op_params = lambdaParams red_op
      (red_acc_params, _) = splitAt (length nes) red_op_params
  forM red_acc_params $ \p ->
    case paramAttr p of
      MemArray pt shape _ (ArrayIn mem _) -> do
        let shape' = Shape [spaceNumThreads space] <> shape
        sArray "red_arr" pt shape' $
          ArrayIn mem $ IxFun.iota $ map (primExpFromSubExp int32) $ shapeDims shape'
      _ -> do
        let pt = elemType $ paramType p
            shape = Shape [spaceGroupSize space]
        sAllocArray "red_arr" pt shape $ Space "local"

-- | Arrays for storing group results.
--
-- The group-result arrays have an extra dimension (of size groupsize)
-- because they are also used for keeping vectorised accumulators for
-- first-stage reduction, if necessary.  When actually storing group
-- results, the first index is set to 0.
groupResultArrays :: KernelSpace -> [SegRedOp InKernel] -> CallKernelGen [[VName]]
groupResultArrays space reds =
  forM reds $ \(SegRedOp _ lam _ shape) ->
    forM (lambdaReturnType lam) $ \t -> do
    let pt = elemType t
        full_shape = Shape [spaceGroupSize space, spaceNumGroups space] <>
                     shape <> arrayShape t
        -- Move the groupsize dimension last to ensure coalesced
        -- memory access.
        perm = [1..shapeRank full_shape-1] ++ [0]
    sAllocArrayPerm "group_res_arr" pt full_shape (Space "device") perm

nonsegmentedReduction :: Pattern ExplicitMemory
                      -> KernelSpace
                      -> [SegRedOp InKernel]
                      -> DoSegBody
                      -> CallKernelGen ()
nonsegmentedReduction segred_pat space reds body = do
  (base_constants, init_constants) <- kernelInitialisationSetSpace space $ return ()
  let constants = base_constants { kernelThreadActive = true }
      global_tid = kernelGlobalThreadId constants
      (_, w) = last $ spaceDimensions space

  counter <-
    sStaticArray "counter" (Space "device") int32 $
    Imp.ArrayValues $ replicate 1 $ IntValue $ Int32Value 0

  -- The group-result arrays have an extra dimension (of size
  -- groupsize) because they are also used for keeping vectorised
  -- accumulators for first-stage reduction, if necessary.  When
  -- actually storing group results, the first index is set to 0.
  reds_group_res_arrs <- groupResultArrays space reds

  num_threads <- dPrimV "num_threads" $ kernelNumThreads constants

  sKernel constants "segred_nonseg" $ allThreads constants $ do
    init_constants

    sync_arr <- sAllocArray "sync_arr" Bool (Shape [intConst Int32 1]) $ Space "local"
    reds_arrs <- mapM (intermediateArrays space) reds

    -- Since this is the nonsegmented case, all outer segment IDs must
    -- necessarily be 0.
    let gtids = map fst $ spaceDimensions space
    forM_ (init gtids) $ \v ->
      v <-- 0

    num_elements <- Imp.elements <$> toExp w
    let elems_per_thread = num_elements `quotRoundingUp` Imp.elements (kernelNumThreads constants)

    slugs <- mapM (segRedOpSlug (kernelLocalThreadId constants) (kernelGroupId constants)) $
             zip3 reds reds_arrs reds_group_res_arrs
    reds_op_renamed <-
      reductionStageOne constants num_elements
      global_tid elems_per_thread num_threads
      slugs body

    let segred_pes = chunks (map (length . segRedNeutral) reds) $
                     patternElements segred_pat
    forM_ (zip6 reds reds_arrs reds_group_res_arrs segred_pes
           slugs reds_op_renamed) $
      \(SegRedOp _ red_op nes _,
        red_arrs, group_res_arrs, pes, slug, red_op_renamed) -> do
      let red_acc_params = take (length nes) $ lambdaParams red_op
      reductionStageTwo constants pes (kernelGroupId constants) 0 [0] 0
        (kernelNumGroups constants) slug red_acc_params red_op_renamed nes
        1 counter sync_arr group_res_arrs red_arrs

smallSegmentsReduction :: Pattern ExplicitMemory
                       -> KernelSpace
                       -> [SegRedOp InKernel]
                       -> DoSegBody
                       -> CallKernelGen ()
smallSegmentsReduction (Pattern _ segred_pes) space reds body = do
  (base_constants, init_constants) <- kernelInitialisationSetSpace space $ return ()
  let constants = base_constants { kernelThreadActive = true }

  let (gtids, dims) = unzip $ spaceDimensions space
  dims' <- mapM toExp dims

  let segment_size = last dims'
  -- Careful to avoid division by zero now.
  segment_size_nonzero_v <- dPrimV "segment_size_nonzero" $
                            BinOpExp (SMax Int32) 1 segment_size
  let segment_size_nonzero = Imp.var segment_size_nonzero_v int32
      num_segments = product $ init dims'
      segments_per_group = kernelGroupSize constants `quot` segment_size_nonzero
      required_groups = num_segments `quotRoundingUp` segments_per_group

  emit $ Imp.DebugPrint "\n# SegRed-small" Nothing
  emit $ Imp.DebugPrint "num_segments" $ Just (int32, num_segments)
  emit $ Imp.DebugPrint "segment_size" $ Just (int32, segment_size)
  emit $ Imp.DebugPrint "segments_per_group" $ Just (int32, segments_per_group)
  emit $ Imp.DebugPrint "required_groups" $ Just (int32, required_groups)

  sKernel constants "segred_small" $ allThreads constants $ do
    init_constants

    reds_arrs <- mapM (intermediateArrays space) reds

    -- We probably do not have enough actual workgroups to cover the
    -- entire iteration space.  Some groups thus have to perform double
    -- duty; we put an outer loop to accomplish this.
    virtualiseGroups constants required_groups $ \group_id_var' -> do
      let group_id' = Imp.vi32 group_id_var'
      -- Compute the 'n' input indices.  The outer 'n-1' correspond to
      -- the segment ID, and are computed from the group id.  The inner
      -- is computed from the local thread id, and may be out-of-bounds.
      let ltid = kernelLocalThreadId constants
          segment_index = (ltid `quot` segment_size_nonzero) + (group_id' * segments_per_group)
          index_within_segment = ltid `rem` segment_size

      zipWithM_ (<--) (init gtids) $ unflattenIndex (init dims') segment_index
      last gtids <-- index_within_segment

      let out_of_bounds =
            forM_ (zip reds reds_arrs) $ \(SegRedOp _ _ nes _, red_arrs) ->
            forM_ (zip red_arrs nes) $ \(arr, ne) ->
            copyDWIM arr [ltid] ne []

          in_bounds =
            body constants $ \red_res ->
            sComment "save results to be reduced" $ do
            let red_dests = zip (concat reds_arrs) $ repeat [ltid]
            forM_ (zip red_dests red_res) $ \((d,d_is), (res, res_is)) ->
              copyDWIM d d_is res res_is

      sComment "apply map function if in bounds" $
        sIf (segment_size .>. 0 .&&.
             isActive (init $ zip gtids dims) .&&.
             ltid .<. segment_size * segments_per_group) in_bounds out_of_bounds

      sOp Imp.LocalBarrier

      let crossesSegment from to = (to-from) .>. (to `rem` segment_size)
      sWhen (segment_size .>. 0) $
        sComment "perform segmented scan to imitate reduction" $
        forM_ (zip reds reds_arrs) $ \(SegRedOp _ red_op _ _, red_arrs) ->
        groupScan constants (Just crossesSegment) (segment_size*segments_per_group) red_op red_arrs

      sOp Imp.LocalBarrier

      sComment "save final values of segments" $
        sWhen (group_id' * segments_per_group + ltid .<. num_segments .&&.
               ltid .<. segments_per_group) $
        forM_ (zip segred_pes (concat reds_arrs)) $ \(pe, arr) -> do
        -- Figure out which segment result this thread should write...
        let flat_segment_index = group_id' * segments_per_group + ltid
            gtids' = unflattenIndex (init dims') flat_segment_index
        copyDWIM (patElemName pe) gtids'
                        (Var arr) [(ltid+1) * segment_size_nonzero - 1]

      -- Finally another barrier, because we will be writing to the
      -- local memory array first thing in the next iteration.
      sOp Imp.LocalBarrier

largeSegmentsReduction :: Pattern ExplicitMemory
                       -> KernelSpace
                       -> [SegRedOp InKernel]
                       -> DoSegBody
                       -> CallKernelGen ()
largeSegmentsReduction segred_pat space reds body = do
  (base_constants, init_constants) <- kernelInitialisationSetSpace space $ return ()
  let (gtids, dims) = unzip $ spaceDimensions space
  dims' <- mapM toExp dims
  let segment_size = last dims'
      num_segments = product $ init dims'

  let (groups_per_segment, elems_per_thread) =
        groupsPerSegmentAndElementsPerThread segment_size num_segments
        (kernelNumGroups base_constants) (kernelGroupSize base_constants)
  virt_num_groups <- dPrimV "virt_num_groups" $
    groups_per_segment * num_segments

  threads_per_segment <- dPrimV "thread_per_segment" $
    groups_per_segment * kernelGroupSize base_constants

  let constants = base_constants { kernelThreadActive = true }

  emit $ Imp.DebugPrint "\n# SegRed-large" Nothing
  emit $ Imp.DebugPrint "num_segments" $ Just (int32, num_segments)
  emit $ Imp.DebugPrint "segment_size" $ Just (int32, segment_size)
  emit $ Imp.DebugPrint "virt_num_groups" $ Just (int32, Imp.vi32 virt_num_groups)
  emit $ Imp.DebugPrint "num_groups" $ Just (int32, kernelNumGroups constants)
  emit $ Imp.DebugPrint "group_size" $ Just (int32, kernelGroupSize constants)
  emit $ Imp.DebugPrint "elems_per_thread" $ Just (int32, Imp.innerExp elems_per_thread)
  emit $ Imp.DebugPrint "groups_per_segment" $ Just (int32, groups_per_segment)

  reds_group_res_arrs <- groupResultArrays space reds

  -- In principle we should have a counter for every segment.  Since
  -- the number of segments is a dynamic quantity, we would have to
  -- allocate and zero out an array here, which is expensive.
  -- However, we exploit the fact that the number of segments being
  -- reduced at any point in time is limited by the number of
  -- workgroups. If we bound the number of workgroups, we can get away
  -- with using that many counters.  FIXME: Is this limit checked
  -- anywhere?  There are other places in the compiler that will fail
  -- if the group count exceeds the maximum group size, which is at
  -- most 1024 anyway.
  let num_counters = 1024
  counter <-
    sStaticArray "counter" (Space "device") int32 $
    Imp.ArrayZeros num_counters

  sKernel constants "segred_large" $ allThreads constants $ do
    init_constants

    reds_arrs <- mapM (intermediateArrays space) reds
    sync_arr <- sAllocArray "sync_arr" Bool (Shape [intConst Int32 1]) $ Space "local"

    -- We probably do not have enough actual workgroups to cover the
    -- entire iteration space.  Some groups thus have to perform double
    -- duty; we put an outer loop to accomplish this.
    virtualiseGroups constants (Imp.vi32 virt_num_groups) $ \group_id_var -> do

      let segment_gtids = init gtids
          group_id = Imp.vi32 group_id_var
          group_size = kernelGroupSize constants
          flat_segment_id = group_id `quot` groups_per_segment
          local_tid = kernelLocalThreadId constants

          global_tid = (group_id * group_size + local_tid)
                       `rem` (group_size * groups_per_segment)
          w = last dims
          first_group_for_segment = flat_segment_id * groups_per_segment

      zipWithM_ (<--) segment_gtids $ unflattenIndex (init dims') flat_segment_id
      num_elements <- Imp.elements <$> toExp w

      slugs <- mapM (segRedOpSlug local_tid group_id) $
               zip3 reds reds_arrs reds_group_res_arrs
      reds_op_renamed <-
        reductionStageOne constants num_elements
        global_tid elems_per_thread threads_per_segment
        slugs body

      let segred_pes = chunks (map (length . segRedNeutral) reds) $
                       patternElements segred_pat

          multiple_groups_per_segment =
            forM_ (zip6 reds reds_arrs reds_group_res_arrs segred_pes
                   slugs reds_op_renamed) $
            \(SegRedOp _ red_op nes _, red_arrs, group_res_arrs, pes,
              slug, red_op_renamed) -> do
              let red_acc_params = take (length nes) $ lambdaParams red_op
              reductionStageTwo constants pes
                group_id flat_segment_id (map (`Imp.var` int32) segment_gtids)
                first_group_for_segment groups_per_segment
                slug red_acc_params red_op_renamed
                nes (fromIntegral num_counters) counter sync_arr group_res_arrs red_arrs

          one_group_per_segment =
            comment "first thread in group saves final result to memory" $
            forM_ (zip slugs segred_pes) $ \(slug, pes) ->
            sWhen (local_tid .==. 0) $
              forM_ (zip pes (slugAccs slug)) $ \(v, (acc, acc_is)) ->
              copyDWIM (patElemName v) (map (`Imp.var` int32) segment_gtids) (Var acc) acc_is

      sIf (groups_per_segment .==. 1) one_group_per_segment multiple_groups_per_segment

-- Careful to avoid division by zero here.
groupsPerSegmentAndElementsPerThread :: Imp.Exp -> Imp.Exp -> Imp.Exp -> Imp.Exp
                                     -> (Imp.Exp, Imp.Count Imp.Elements)
groupsPerSegmentAndElementsPerThread segment_size num_segments num_groups_hint group_size =
  let groups_per_segment =
        num_groups_hint `quotRoundingUp` BinOpExp (SMax Int32) 1 num_segments
      elements_per_thread =
        segment_size `quotRoundingUp` (group_size * groups_per_segment)
  in (groups_per_segment, Imp.elements elements_per_thread)

-- | A SegRedOp with auxiliary information.
data SegRedOpSlug =
  SegRedOpSlug
  { slugOp :: SegRedOp InKernel
  , slugArrs :: [VName]
    -- ^ The arrays used for computing the intra-group reduction
    -- (either local or global memory).
  , slugAccs :: [(VName, [Imp.Exp])]
    -- ^ Places to store accumulator in stage 1 reduction.
  }

slugBody :: SegRedOpSlug -> Body InKernel
slugBody = lambdaBody . segRedLambda . slugOp

slugParams :: SegRedOpSlug -> [LParam InKernel]
slugParams = lambdaParams . segRedLambda . slugOp

slugNeutral :: SegRedOpSlug -> [SubExp]
slugNeutral = segRedNeutral . slugOp

slugShape :: SegRedOpSlug -> Shape
slugShape = segRedShape . slugOp

slugsComm :: [SegRedOpSlug] -> Commutativity
slugsComm = mconcat . map (segRedComm . slugOp)

accParams, nextParams :: SegRedOpSlug -> [LParam InKernel]
accParams slug = take (length (slugNeutral slug)) $ slugParams slug
nextParams slug = drop (length (slugNeutral slug)) $ slugParams slug

segRedOpSlug :: Imp.Exp -> Imp.Exp -> (SegRedOp InKernel, [VName], [VName]) -> InKernelGen SegRedOpSlug
segRedOpSlug local_tid group_id (op, group_res_arrs, param_arrs) =
  SegRedOpSlug op group_res_arrs <$>
  zipWithM mkAcc (lambdaParams (segRedLambda op)) param_arrs
  where mkAcc p param_arr
          | Prim t <- paramType p,
            shapeRank (segRedShape op) == 0 = do
              acc <- dPrim (baseString (paramName p) <> "_acc") t
              return (acc, [])
          | otherwise =
              return (param_arr, [local_tid, group_id])

reductionStageZero :: KernelConstants
                   -> Imp.Count Imp.Elements
                   -> Imp.Exp
                   -> Imp.Count Imp.Elements
                   -> VName
                   -> [SegRedOpSlug]
                   -> DoSegBody
                   -> InKernelGen ([Lambda InKernel], InKernelGen ())
reductionStageZero constants num_elements global_tid elems_per_thread threads_per_segment slugs body = do

  let (gtids, _dims) = unzip $ kernelDimensions constants
      gtid = last gtids
      local_tid = kernelLocalThreadId constants

  -- Figure out how many elements this thread should process.
  chunk_size <- dPrim "chunk_size" int32
  let ordering = case slugsComm slugs of
                   Commutative -> SplitStrided $ Var threads_per_segment
                   Noncommutative -> SplitContiguous
  computeThreadChunkSize ordering global_tid elems_per_thread num_elements chunk_size

  dScope Nothing $ scopeOfLParams $ concatMap slugParams slugs

  sComment "neutral-initialise the accumulators" $
    forM_ slugs $ \slug ->
    forM_ (zip (slugAccs slug) (slugNeutral slug)) $ \((acc, acc_is), ne) ->
    sLoopNest (slugShape slug) $ \vec_is ->
    copyDWIM acc (acc_is++vec_is) ne []

  slugs_op_renamed <- mapM (renameLambda . segRedLambda . slugOp) slugs

  let doTheReduction =
        forM_ (zip slugs_op_renamed slugs) $ \(slug_op_renamed, slug) ->
        sLoopNest (slugShape slug) $ \vec_is -> do
          comment "to reduce current chunk, first store our result in memory" $ do
            forM_ (zip (slugParams slug) (slugAccs slug)) $ \(p, (acc, acc_is)) ->
              copyDWIM (paramName p) [] (Var acc) (acc_is++vec_is)

            forM_ (zip (slugArrs slug) (slugParams slug)) $ \(arr, p) ->
              when (primType $ paramType p) $
              copyDWIM arr [local_tid] (Var $ paramName p) []

          sOp Imp.LocalBarrier

          groupReduce constants (kernelGroupSize constants) slug_op_renamed (slugArrs slug)

          sOp Imp.LocalBarrier

          sComment "first thread saves the result in accumulator" $
            sWhen (local_tid .==. 0) $
            forM_ (zip (slugAccs slug) (lambdaParams slug_op_renamed)) $ \((acc, acc_is), p) ->
            copyDWIM acc (acc_is++vec_is) (Var $ paramName p) []

  i <- newVName "i"
  -- If this is a non-commutative reduction, each thread must run the
  -- loop the same number of iterations, because we will be performing
  -- a group-wide reduction in there.
  let comm = slugsComm slugs
      (bound, check_bounds) =
        case comm of
          Commutative -> (Imp.var chunk_size int32, id)
          Noncommutative -> (Imp.innerExp elems_per_thread,
                             sWhen (Imp.var gtid int32 .<. Imp.innerExp num_elements))

  sFor i Int32 bound $ do
    gtid <--
      case comm of
        Commutative ->
          global_tid +
          Imp.var threads_per_segment int32 * Imp.var i int32
        Noncommutative ->
          let index_in_segment = global_tid `quot` kernelGroupSize constants
          in local_tid +
             (index_in_segment * Imp.innerExp elems_per_thread + Imp.var i int32) *
             kernelGroupSize constants

    check_bounds $ sComment "apply map function" $
      body constants $ \all_red_res -> do

      let slugs_res = chunks (map (length . slugNeutral) slugs) all_red_res

      forM_ (zip slugs slugs_res) $ \(slug, red_res) ->
        sLoopNest (slugShape slug) $ \vec_is -> do
        sComment "load accumulator" $
          forM_ (zip (accParams slug) (slugAccs slug)) $ \(p, (acc, acc_is)) ->
          copyDWIM (paramName p) [] (Var acc) (acc_is ++ vec_is)
        sComment "load new values" $
          forM_ (zip (nextParams slug) red_res) $ \(p, (res, res_is)) ->
          copyDWIM (paramName p) [] res (res_is ++ vec_is)
        sComment "apply reduction operator" $
          compileStms mempty (bodyStms $ slugBody slug) $
          sComment "store in accumulator" $
          forM_ (zip
                  (slugAccs slug)
                  (bodyResult $ slugBody slug)) $ \((acc, acc_is), se) ->
          copyDWIM acc (acc_is ++ vec_is) se []

    case comm of
      Noncommutative -> do
        doTheReduction
        sComment "first thread keeps accumulator; others reset to neutral element" $ do
          let reset_to_neutral =
                forM_ slugs $ \slug ->
                forM_ (zip (slugAccs slug) (slugNeutral slug)) $ \((acc, acc_is), ne) ->
                sLoopNest (slugShape slug) $ \vec_is ->
                copyDWIM acc (acc_is++vec_is) ne []
          sUnless (local_tid .==. 0) reset_to_neutral
      _ -> return ()

  return (slugs_op_renamed, doTheReduction)

reductionStageOne :: KernelConstants
                  -> Imp.Count Imp.Elements
                  -> Imp.Exp
                  -> Imp.Count Imp.Elements
                  -> VName
                  -> [SegRedOpSlug]
                  -> DoSegBody
                  -> InKernelGen [Lambda InKernel]
reductionStageOne constants num_elements global_tid elems_per_thread threads_per_segment slugs body = do
  (slugs_op_renamed, doTheReduction) <-
    reductionStageZero constants num_elements global_tid elems_per_thread threads_per_segment slugs body

  case slugsComm slugs of
    Noncommutative ->
      forM_ slugs $ \slug ->
      forM_ (zip (accParams slug) (slugAccs slug)) $ \(p, (acc, acc_is)) ->
      copyDWIM (paramName p) [] (Var acc) acc_is
    _ -> doTheReduction

  return slugs_op_renamed

reductionStageTwo :: KernelConstants
                  -> [PatElem ExplicitMemory]
                  -> Imp.Exp
                  -> Imp.Exp
                  -> [Imp.Exp]
                  -> Imp.Exp
                  -> Imp.Exp
                  -> SegRedOpSlug
                  -> [LParam InKernel]
                  -> Lambda InKernel
                  -> [SubExp]
                  -> Imp.Exp
                  -> VName
                  -> VName
                  -> [VName]
                  -> [VName]
                  -> InKernelGen ()
reductionStageTwo constants segred_pes
                  group_id flat_segment_id segment_gtids first_group_for_segment groups_per_segment
                  slug red_acc_params
                  red_op_renamed nes
                  num_counters counter sync_arr group_res_arrs red_arrs = do
  let local_tid = kernelLocalThreadId constants
      group_size = kernelGroupSize constants
  old_counter <- dPrim "old_counter" int32
  (counter_mem, _, counter_offset) <- fullyIndexArray counter [flat_segment_id `rem` num_counters]
  comment "first thread in group saves group result to global memory" $
    sWhen (local_tid .==. 0) $ do
    forM_ (take (length nes) $ zip group_res_arrs (slugAccs slug)) $ \(v, (acc, acc_is)) ->
      copyDWIM v [0, group_id] (Var acc) acc_is
    sOp Imp.MemFenceGlobal
    -- Increment the counter, thus stating that our result is
    -- available.
    sOp $ Imp.Atomic DefaultSpace $ Imp.AtomicAdd old_counter counter_mem counter_offset 1
    -- Now check if we were the last group to write our result.  If
    -- so, it is our responsibility to produce the final result.
    sWrite sync_arr [0] $ Imp.var old_counter int32 .==. groups_per_segment - 1

  sOp Imp.LocalBarrier

  is_last_group <- dPrim "is_last_group" Bool
  copyDWIM is_last_group [] (Var sync_arr) [0]
  sWhen (Imp.var is_last_group Bool) $ do
    -- The final group has written its result (and it was
    -- us!), so read in all the group results and perform the
    -- final stage of the reduction.  But first, we reset the
    -- counter so it is ready for next time.  This is done
    -- with an atomic to avoid warnings about write/write
    -- races in oclgrind.
    sWhen (local_tid .==. 0) $
      sOp $ Imp.Atomic DefaultSpace $ Imp.AtomicAdd old_counter counter_mem counter_offset $
      negate groups_per_segment
    sLoopNest (slugShape slug) $ \vec_is -> do
      comment "read in the per-group-results" $
        forM_ (zip4 red_acc_params red_arrs nes group_res_arrs) $
        \(p, arr, ne, group_res_arr) -> do
          let load_group_result =
                copyDWIM (paramName p) []
                (Var group_res_arr) ([0, first_group_for_segment + local_tid] ++ vec_is)
              load_neutral_element =
                copyDWIM (paramName p) [] ne []
          sIf (local_tid .<. groups_per_segment)
            load_group_result load_neutral_element
          when (primType $ paramType p) $
            copyDWIM arr [local_tid] (Var $ paramName p) []

      sOp Imp.LocalBarrier

      sComment "reduce the per-group results" $ do
        groupReduce constants group_size red_op_renamed red_arrs

        sComment "and back to memory with the final result" $
          sWhen (local_tid .==. 0) $
          forM_ (zip segred_pes $ lambdaParams red_op_renamed) $ \(pe, p) ->
          copyDWIM (patElemName pe) (segment_gtids++vec_is) (Var $ paramName p) []
