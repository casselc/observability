---------------------------- MODULE counterexample ----------------------------

EXTENDS currentDesign

(* Constant initialization state *)
ConstInit == TRUE

(* Initial state [_transition(0)] *)
State0 ==
  currentDesign_edgePublish_centralCount
      = SetAsFun({ <<[batch |-> 1, epoch |-> 1], 0>>,
        <<[batch |-> 1, epoch |-> 2], 0>>,
        <<[batch |-> 2, epoch |-> 1], 0>>,
        <<[batch |-> 2, epoch |-> 2], 0>>,
        <<[batch |-> 3, epoch |-> 1], 0>>,
        <<[batch |-> 3, epoch |-> 2], 0>> })
    /\ currentDesign_edgePublish_dedupWindow = <<>>
    /\ currentDesign_edgePublish_events = {}
    /\ currentDesign_edgePublish_ledger = {}
    /\ currentDesign_edgePublish_ledgerContent = {}
    /\ currentDesign_edgePublish_manifests = {}
    /\ currentDesign_edgePublish_parquet = {}
    /\ currentDesign_edgePublish_queue = { 1, 2 }
    /\ currentDesign_edgePublish_seals = {}
    /\ currentDesign_edgePublish_tableRows = {}
    /\ currentDesign_edgePublish_workers
      = SetAsFun({ <<1, [claim |-> {}, inserted |-> FALSE]>>,
        <<2, [claim |-> {}, inserted |-> FALSE]>> })
    /\ currentDesign_edgePublish_writer
      = [epoch |-> 1,
        gen |-> 1,
        nextBatch |-> 1,
        pushes |-> {},
        rotated |-> {},
        succeeded |-> {}]

(* State1 [_transition(0)] *)
State1 ==
  currentDesign_edgePublish_centralCount
      = SetAsFun({ <<[batch |-> 1, epoch |-> 1], 0>>,
        <<[batch |-> 1, epoch |-> 2], 0>>,
        <<[batch |-> 2, epoch |-> 1], 0>>,
        <<[batch |-> 2, epoch |-> 2], 0>>,
        <<[batch |-> 3, epoch |-> 1], 0>>,
        <<[batch |-> 3, epoch |-> 2], 0>> })
    /\ currentDesign_edgePublish_dedupWindow = <<>>
    /\ currentDesign_edgePublish_events = {}
    /\ currentDesign_edgePublish_ledger = {}
    /\ currentDesign_edgePublish_ledgerContent = {}
    /\ currentDesign_edgePublish_manifests = {}
    /\ currentDesign_edgePublish_parquet = {}
    /\ currentDesign_edgePublish_queue = { 1, 2 }
    /\ currentDesign_edgePublish_seals = {}
    /\ currentDesign_edgePublish_tableRows = {}
    /\ currentDesign_edgePublish_workers
      = SetAsFun({ <<1, [claim |-> {}, inserted |-> FALSE]>>,
        <<2, [claim |-> {}, inserted |-> FALSE]>> })
    /\ currentDesign_edgePublish_writer
      = [epoch |-> 1,
        gen |-> 1,
        nextBatch |-> 2,
        pushes |->
          {[bk |-> [batch |-> 1, epoch |-> 1],
            gk |-> [epoch |-> 1, gen |-> 1],
            payload |-> 2,
            phase |-> Variant("Encoded", [tag |-> "UNIT"])]},
        rotated |-> {},
        succeeded |-> {}]

(* State2 [_transition(1)] *)
State2 ==
  currentDesign_edgePublish_centralCount
      = SetAsFun({ <<[batch |-> 1, epoch |-> 1], 0>>,
        <<[batch |-> 1, epoch |-> 2], 0>>,
        <<[batch |-> 2, epoch |-> 1], 0>>,
        <<[batch |-> 2, epoch |-> 2], 0>>,
        <<[batch |-> 3, epoch |-> 1], 0>>,
        <<[batch |-> 3, epoch |-> 2], 0>> })
    /\ currentDesign_edgePublish_dedupWindow = <<>>
    /\ currentDesign_edgePublish_events = {}
    /\ currentDesign_edgePublish_ledger = {}
    /\ currentDesign_edgePublish_ledgerContent = {}
    /\ currentDesign_edgePublish_manifests = {}
    /\ currentDesign_edgePublish_parquet = {}
    /\ currentDesign_edgePublish_queue = { 1, 2 }
    /\ currentDesign_edgePublish_seals = {}
    /\ currentDesign_edgePublish_tableRows
      = {[bk |-> [batch |-> 1, epoch |-> 1], payload |-> 2]}
    /\ currentDesign_edgePublish_workers
      = SetAsFun({ <<1, [claim |-> {}, inserted |-> FALSE]>>,
        <<2, [claim |-> {}, inserted |-> FALSE]>> })
    /\ currentDesign_edgePublish_writer
      = [epoch |-> 1,
        gen |-> 1,
        nextBatch |-> 2,
        pushes |->
          {[bk |-> [batch |-> 1, epoch |-> 1],
            gk |-> [epoch |-> 1, gen |-> 1],
            payload |-> 2,
            phase |-> Variant("TableDone", [tag |-> "UNIT"])]},
        rotated |-> {},
        succeeded |-> {}]

(* State3 [_transition(2)] *)
State3 ==
  currentDesign_edgePublish_centralCount
      = SetAsFun({ <<[batch |-> 1, epoch |-> 1], 0>>,
        <<[batch |-> 1, epoch |-> 2], 0>>,
        <<[batch |-> 2, epoch |-> 1], 0>>,
        <<[batch |-> 2, epoch |-> 2], 0>>,
        <<[batch |-> 3, epoch |-> 1], 0>>,
        <<[batch |-> 3, epoch |-> 2], 0>> })
    /\ currentDesign_edgePublish_dedupWindow = <<>>
    /\ currentDesign_edgePublish_events = {}
    /\ currentDesign_edgePublish_ledger = {}
    /\ currentDesign_edgePublish_ledgerContent = {}
    /\ currentDesign_edgePublish_manifests = {}
    /\ currentDesign_edgePublish_parquet = {[batch |-> 1, epoch |-> 1]}
    /\ currentDesign_edgePublish_queue = { 1, 2 }
    /\ currentDesign_edgePublish_seals = {}
    /\ currentDesign_edgePublish_tableRows
      = {[bk |-> [batch |-> 1, epoch |-> 1], payload |-> 2]}
    /\ currentDesign_edgePublish_workers
      = SetAsFun({ <<1, [claim |-> {}, inserted |-> FALSE]>>,
        <<2, [claim |-> {}, inserted |-> FALSE]>> })
    /\ currentDesign_edgePublish_writer
      = [epoch |-> 1,
        gen |-> 1,
        nextBatch |-> 2,
        pushes |->
          {[bk |-> [batch |-> 1, epoch |-> 1],
            gk |-> [epoch |-> 1, gen |-> 1],
            payload |-> 2,
            phase |-> Variant("ParquetDone", [tag |-> "UNIT"])]},
        rotated |-> {},
        succeeded |-> {}]

(* State4 [_transition(3)] *)
State4 ==
  currentDesign_edgePublish_centralCount
      = SetAsFun({ <<[batch |-> 1, epoch |-> 1], 0>>,
        <<[batch |-> 1, epoch |-> 2], 0>>,
        <<[batch |-> 2, epoch |-> 1], 0>>,
        <<[batch |-> 2, epoch |-> 2], 0>>,
        <<[batch |-> 3, epoch |-> 1], 0>>,
        <<[batch |-> 3, epoch |-> 2], 0>> })
    /\ currentDesign_edgePublish_dedupWindow = <<>>
    /\ currentDesign_edgePublish_events = {"ambiguous"}
    /\ currentDesign_edgePublish_ledger = {}
    /\ currentDesign_edgePublish_ledgerContent = {}
    /\ currentDesign_edgePublish_manifests
      = {[bk |-> [batch |-> 1, epoch |-> 1],
        gk |-> [epoch |-> 1, gen |-> 1],
        payload |-> 2]}
    /\ currentDesign_edgePublish_parquet = {[batch |-> 1, epoch |-> 1]}
    /\ currentDesign_edgePublish_queue = { 1, 2 }
    /\ currentDesign_edgePublish_seals = {}
    /\ currentDesign_edgePublish_tableRows
      = {[bk |-> [batch |-> 1, epoch |-> 1], payload |-> 2]}
    /\ currentDesign_edgePublish_workers
      = SetAsFun({ <<1, [claim |-> {}, inserted |-> FALSE]>>,
        <<2, [claim |-> {}, inserted |-> FALSE]>> })
    /\ currentDesign_edgePublish_writer
      = [epoch |-> 1,
        gen |-> 1,
        nextBatch |-> 2,
        pushes |-> {},
        rotated |-> {},
        succeeded |-> {}]

(* State5 [_transition(4)] *)
State5 ==
  currentDesign_edgePublish_centralCount
      = SetAsFun({ <<[batch |-> 1, epoch |-> 1], 0>>,
        <<[batch |-> 1, epoch |-> 2], 0>>,
        <<[batch |-> 2, epoch |-> 1], 0>>,
        <<[batch |-> 2, epoch |-> 2], 0>>,
        <<[batch |-> 3, epoch |-> 1], 0>>,
        <<[batch |-> 3, epoch |-> 2], 0>> })
    /\ currentDesign_edgePublish_dedupWindow = <<>>
    /\ currentDesign_edgePublish_events = {"ambiguous"}
    /\ currentDesign_edgePublish_ledger = {}
    /\ currentDesign_edgePublish_ledgerContent = {}
    /\ currentDesign_edgePublish_manifests
      = {[bk |-> [batch |-> 1, epoch |-> 1],
        gk |-> [epoch |-> 1, gen |-> 1],
        payload |-> 2]}
    /\ currentDesign_edgePublish_parquet = {[batch |-> 1, epoch |-> 1]}
    /\ currentDesign_edgePublish_queue = { 1, 2 }
    /\ currentDesign_edgePublish_seals = {}
    /\ currentDesign_edgePublish_tableRows
      = {[bk |-> [batch |-> 1, epoch |-> 1], payload |-> 2]}
    /\ currentDesign_edgePublish_workers
      = SetAsFun({ <<1, [claim |-> {}, inserted |-> FALSE]>>,
        <<2, [claim |-> {}, inserted |-> FALSE]>> })
    /\ currentDesign_edgePublish_writer
      = [epoch |-> 1,
        gen |-> 2,
        nextBatch |-> 2,
        pushes |-> {},
        rotated |-> {[epoch |-> 1, gen |-> 1]},
        succeeded |-> {}]

(* State6 [_transition(5)] *)
State6 ==
  currentDesign_edgePublish_centralCount
      = SetAsFun({ <<[batch |-> 1, epoch |-> 1], 0>>,
        <<[batch |-> 1, epoch |-> 2], 0>>,
        <<[batch |-> 2, epoch |-> 1], 0>>,
        <<[batch |-> 2, epoch |-> 2], 0>>,
        <<[batch |-> 3, epoch |-> 1], 0>>,
        <<[batch |-> 3, epoch |-> 2], 0>> })
    /\ currentDesign_edgePublish_dedupWindow = <<>>
    /\ currentDesign_edgePublish_events = { "ambiguous", "sealed" }
    /\ currentDesign_edgePublish_ledger = {}
    /\ currentDesign_edgePublish_ledgerContent = {}
    /\ currentDesign_edgePublish_manifests
      = {[bk |-> [batch |-> 1, epoch |-> 1],
        gk |-> [epoch |-> 1, gen |-> 1],
        payload |-> 2]}
    /\ currentDesign_edgePublish_parquet = {[batch |-> 1, epoch |-> 1]}
    /\ currentDesign_edgePublish_queue = { 1, 2 }
    /\ currentDesign_edgePublish_seals
      = {[batches |-> {}, gk |-> [epoch |-> 1, gen |-> 1]]}
    /\ currentDesign_edgePublish_tableRows
      = {[bk |-> [batch |-> 1, epoch |-> 1], payload |-> 2]}
    /\ currentDesign_edgePublish_workers
      = SetAsFun({ <<1, [claim |-> {}, inserted |-> FALSE]>>,
        <<2, [claim |-> {}, inserted |-> FALSE]>> })
    /\ currentDesign_edgePublish_writer
      = [epoch |-> 1,
        gen |-> 2,
        nextBatch |-> 2,
        pushes |-> {},
        rotated |-> {},
        succeeded |-> {}]

(* The following formula holds true in the last state and violates the invariant *)
InvariantViolation ==
  Skolem((\E currentDesign_edgePublish_s_1700_2 \in currentDesign_edgePublish_seals:
    ~(currentDesign_edgePublish_s_1700_2["batches"]
      = {
        currentDesign_edgePublish_m_1697_2["bk"]:
          currentDesign_edgePublish_m_1697_2 \in
            {
              currentDesign_edgePublish_m_1691_2 \in
                currentDesign_edgePublish_manifests:
                currentDesign_edgePublish_m_1691_2["gk"]
                  = currentDesign_edgePublish_s_1700_2["gk"]
            }
      })))

================================================================================
(* Created by Apalache on Thu Sep 24 21:44:19 UTC 2026 *)
(* https://github.com/apalache-mc/apalache *)
