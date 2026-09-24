---------------------------- MODULE counterexample ----------------------------

EXTENDS noRotationLock

(* Constant initialization state *)
ConstInit == TRUE

(* Initial state [_transition(0)] *)
State0 ==
  noRotationLock_edgePublish_centralCount
      = SetAsFun({ <<[batch |-> 1, epoch |-> 1], 0>>,
        <<[batch |-> 1, epoch |-> 2], 0>>,
        <<[batch |-> 2, epoch |-> 1], 0>>,
        <<[batch |-> 2, epoch |-> 2], 0>>,
        <<[batch |-> 3, epoch |-> 1], 0>>,
        <<[batch |-> 3, epoch |-> 2], 0>> })
    /\ noRotationLock_edgePublish_dedupWindow = <<>>
    /\ noRotationLock_edgePublish_events = {}
    /\ noRotationLock_edgePublish_ledger = {}
    /\ noRotationLock_edgePublish_ledgerContent = {}
    /\ noRotationLock_edgePublish_manifests = {}
    /\ noRotationLock_edgePublish_parquet = {}
    /\ noRotationLock_edgePublish_queue = { 1, 2 }
    /\ noRotationLock_edgePublish_seals = {}
    /\ noRotationLock_edgePublish_tableRows = {}
    /\ noRotationLock_edgePublish_workers
      = SetAsFun({ <<1, [claim |-> {}, inserted |-> FALSE]>>,
        <<2, [claim |-> {}, inserted |-> FALSE]>> })
    /\ noRotationLock_edgePublish_writer
      = [epoch |-> 1,
        gen |-> 1,
        nextBatch |-> 1,
        pushes |-> {},
        rotated |-> {},
        succeeded |-> {}]

(* State1 [_transition(0)] *)
State1 ==
  noRotationLock_edgePublish_centralCount
      = SetAsFun({ <<[batch |-> 1, epoch |-> 1], 0>>,
        <<[batch |-> 1, epoch |-> 2], 0>>,
        <<[batch |-> 2, epoch |-> 1], 0>>,
        <<[batch |-> 2, epoch |-> 2], 0>>,
        <<[batch |-> 3, epoch |-> 1], 0>>,
        <<[batch |-> 3, epoch |-> 2], 0>> })
    /\ noRotationLock_edgePublish_dedupWindow = <<>>
    /\ noRotationLock_edgePublish_events = {}
    /\ noRotationLock_edgePublish_ledger = {}
    /\ noRotationLock_edgePublish_ledgerContent = {}
    /\ noRotationLock_edgePublish_manifests = {}
    /\ noRotationLock_edgePublish_parquet = {}
    /\ noRotationLock_edgePublish_queue = { 1, 2 }
    /\ noRotationLock_edgePublish_seals = {}
    /\ noRotationLock_edgePublish_tableRows = {}
    /\ noRotationLock_edgePublish_workers
      = SetAsFun({ <<1, [claim |-> {}, inserted |-> FALSE]>>,
        <<2, [claim |-> {}, inserted |-> FALSE]>> })
    /\ noRotationLock_edgePublish_writer
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

(* State2 [_transition(4)] *)
State2 ==
  noRotationLock_edgePublish_centralCount
      = SetAsFun({ <<[batch |-> 1, epoch |-> 1], 0>>,
        <<[batch |-> 1, epoch |-> 2], 0>>,
        <<[batch |-> 2, epoch |-> 1], 0>>,
        <<[batch |-> 2, epoch |-> 2], 0>>,
        <<[batch |-> 3, epoch |-> 1], 0>>,
        <<[batch |-> 3, epoch |-> 2], 0>> })
    /\ noRotationLock_edgePublish_dedupWindow = <<>>
    /\ noRotationLock_edgePublish_events = {}
    /\ noRotationLock_edgePublish_ledger = {}
    /\ noRotationLock_edgePublish_ledgerContent = {}
    /\ noRotationLock_edgePublish_manifests = {}
    /\ noRotationLock_edgePublish_parquet = {}
    /\ noRotationLock_edgePublish_queue = { 1, 2 }
    /\ noRotationLock_edgePublish_seals = {}
    /\ noRotationLock_edgePublish_tableRows = {}
    /\ noRotationLock_edgePublish_workers
      = SetAsFun({ <<1, [claim |-> {}, inserted |-> FALSE]>>,
        <<2, [claim |-> {}, inserted |-> FALSE]>> })
    /\ noRotationLock_edgePublish_writer
      = [epoch |-> 1,
        gen |-> 2,
        nextBatch |-> 2,
        pushes |->
          {[bk |-> [batch |-> 1, epoch |-> 1],
            gk |-> [epoch |-> 1, gen |-> 1],
            payload |-> 2,
            phase |-> Variant("Encoded", [tag |-> "UNIT"])]},
        rotated |-> {[epoch |-> 1, gen |-> 1]},
        succeeded |-> {}]

(* State3 [_transition(1)] *)
State3 ==
  noRotationLock_edgePublish_centralCount
      = SetAsFun({ <<[batch |-> 1, epoch |-> 1], 0>>,
        <<[batch |-> 1, epoch |-> 2], 0>>,
        <<[batch |-> 2, epoch |-> 1], 0>>,
        <<[batch |-> 2, epoch |-> 2], 0>>,
        <<[batch |-> 3, epoch |-> 1], 0>>,
        <<[batch |-> 3, epoch |-> 2], 0>> })
    /\ noRotationLock_edgePublish_dedupWindow = <<>>
    /\ noRotationLock_edgePublish_events = {}
    /\ noRotationLock_edgePublish_ledger = {}
    /\ noRotationLock_edgePublish_ledgerContent = {}
    /\ noRotationLock_edgePublish_manifests = {}
    /\ noRotationLock_edgePublish_parquet = {}
    /\ noRotationLock_edgePublish_queue = { 1, 2 }
    /\ noRotationLock_edgePublish_seals = {}
    /\ noRotationLock_edgePublish_tableRows
      = {[bk |-> [batch |-> 1, epoch |-> 1], payload |-> 2]}
    /\ noRotationLock_edgePublish_workers
      = SetAsFun({ <<1, [claim |-> {}, inserted |-> FALSE]>>,
        <<2, [claim |-> {}, inserted |-> FALSE]>> })
    /\ noRotationLock_edgePublish_writer
      = [epoch |-> 1,
        gen |-> 2,
        nextBatch |-> 2,
        pushes |->
          {[bk |-> [batch |-> 1, epoch |-> 1],
            gk |-> [epoch |-> 1, gen |-> 1],
            payload |-> 2,
            phase |-> Variant("TableDone", [tag |-> "UNIT"])]},
        rotated |-> {[epoch |-> 1, gen |-> 1]},
        succeeded |-> {}]

(* State4 [_transition(2)] *)
State4 ==
  noRotationLock_edgePublish_centralCount
      = SetAsFun({ <<[batch |-> 1, epoch |-> 1], 0>>,
        <<[batch |-> 1, epoch |-> 2], 0>>,
        <<[batch |-> 2, epoch |-> 1], 0>>,
        <<[batch |-> 2, epoch |-> 2], 0>>,
        <<[batch |-> 3, epoch |-> 1], 0>>,
        <<[batch |-> 3, epoch |-> 2], 0>> })
    /\ noRotationLock_edgePublish_dedupWindow = <<>>
    /\ noRotationLock_edgePublish_events = {}
    /\ noRotationLock_edgePublish_ledger = {}
    /\ noRotationLock_edgePublish_ledgerContent = {}
    /\ noRotationLock_edgePublish_manifests = {}
    /\ noRotationLock_edgePublish_parquet = {[batch |-> 1, epoch |-> 1]}
    /\ noRotationLock_edgePublish_queue = { 1, 2 }
    /\ noRotationLock_edgePublish_seals = {}
    /\ noRotationLock_edgePublish_tableRows
      = {[bk |-> [batch |-> 1, epoch |-> 1], payload |-> 2]}
    /\ noRotationLock_edgePublish_workers
      = SetAsFun({ <<1, [claim |-> {}, inserted |-> FALSE]>>,
        <<2, [claim |-> {}, inserted |-> FALSE]>> })
    /\ noRotationLock_edgePublish_writer
      = [epoch |-> 1,
        gen |-> 2,
        nextBatch |-> 2,
        pushes |->
          {[bk |-> [batch |-> 1, epoch |-> 1],
            gk |-> [epoch |-> 1, gen |-> 1],
            payload |-> 2,
            phase |-> Variant("ParquetDone", [tag |-> "UNIT"])]},
        rotated |-> {[epoch |-> 1, gen |-> 1]},
        succeeded |-> {}]

(* State5 [_transition(5)] *)
State5 ==
  noRotationLock_edgePublish_centralCount
      = SetAsFun({ <<[batch |-> 1, epoch |-> 1], 0>>,
        <<[batch |-> 1, epoch |-> 2], 0>>,
        <<[batch |-> 2, epoch |-> 1], 0>>,
        <<[batch |-> 2, epoch |-> 2], 0>>,
        <<[batch |-> 3, epoch |-> 1], 0>>,
        <<[batch |-> 3, epoch |-> 2], 0>> })
    /\ noRotationLock_edgePublish_dedupWindow = <<>>
    /\ noRotationLock_edgePublish_events = {"sealed"}
    /\ noRotationLock_edgePublish_ledger = {}
    /\ noRotationLock_edgePublish_ledgerContent = {}
    /\ noRotationLock_edgePublish_manifests = {}
    /\ noRotationLock_edgePublish_parquet = {[batch |-> 1, epoch |-> 1]}
    /\ noRotationLock_edgePublish_queue = { 1, 2 }
    /\ noRotationLock_edgePublish_seals
      = {[batches |-> {}, gk |-> [epoch |-> 1, gen |-> 1]]}
    /\ noRotationLock_edgePublish_tableRows
      = {[bk |-> [batch |-> 1, epoch |-> 1], payload |-> 2]}
    /\ noRotationLock_edgePublish_workers
      = SetAsFun({ <<1, [claim |-> {}, inserted |-> FALSE]>>,
        <<2, [claim |-> {}, inserted |-> FALSE]>> })
    /\ noRotationLock_edgePublish_writer
      = [epoch |-> 1,
        gen |-> 2,
        nextBatch |-> 2,
        pushes |->
          {[bk |-> [batch |-> 1, epoch |-> 1],
            gk |-> [epoch |-> 1, gen |-> 1],
            payload |-> 2,
            phase |-> Variant("ParquetDone", [tag |-> "UNIT"])]},
        rotated |-> {},
        succeeded |-> {}]

(* State6 [_transition(3)] *)
State6 ==
  noRotationLock_edgePublish_centralCount
      = SetAsFun({ <<[batch |-> 1, epoch |-> 1], 0>>,
        <<[batch |-> 1, epoch |-> 2], 0>>,
        <<[batch |-> 2, epoch |-> 1], 0>>,
        <<[batch |-> 2, epoch |-> 2], 0>>,
        <<[batch |-> 3, epoch |-> 1], 0>>,
        <<[batch |-> 3, epoch |-> 2], 0>> })
    /\ noRotationLock_edgePublish_dedupWindow = <<>>
    /\ noRotationLock_edgePublish_events = {"sealed"}
    /\ noRotationLock_edgePublish_ledger = {}
    /\ noRotationLock_edgePublish_ledgerContent = {}
    /\ noRotationLock_edgePublish_manifests
      = {[bk |-> [batch |-> 1, epoch |-> 1],
        gk |-> [epoch |-> 1, gen |-> 1],
        payload |-> 2]}
    /\ noRotationLock_edgePublish_parquet = {[batch |-> 1, epoch |-> 1]}
    /\ noRotationLock_edgePublish_queue = {1}
    /\ noRotationLock_edgePublish_seals
      = {[batches |-> {}, gk |-> [epoch |-> 1, gen |-> 1]]}
    /\ noRotationLock_edgePublish_tableRows
      = {[bk |-> [batch |-> 1, epoch |-> 1], payload |-> 2]}
    /\ noRotationLock_edgePublish_workers
      = SetAsFun({ <<1, [claim |-> {}, inserted |-> FALSE]>>,
        <<2, [claim |-> {}, inserted |-> FALSE]>> })
    /\ noRotationLock_edgePublish_writer
      = [epoch |-> 1,
        gen |-> 2,
        nextBatch |-> 2,
        pushes |-> {},
        rotated |-> {},
        succeeded |->
          {[bk |-> [batch |-> 1, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]]}]

(* The following formula holds true in the last state and violates the invariant *)
InvariantViolation ==
  Skolem((\E noRotationLock_edgePublish_s_1700_2 \in noRotationLock_edgePublish_seals:
    ~(noRotationLock_edgePublish_s_1700_2["batches"]
      = {
        noRotationLock_edgePublish_m_1697_2["bk"]:
          noRotationLock_edgePublish_m_1697_2 \in
            {
              noRotationLock_edgePublish_m_1691_2 \in
                noRotationLock_edgePublish_manifests:
                noRotationLock_edgePublish_m_1691_2["gk"]
                  = noRotationLock_edgePublish_s_1700_2["gk"]
            }
      })))

================================================================================
(* Created by Apalache on Thu Sep 24 21:44:41 UTC 2026 *)
(* https://github.com/apalache-mc/apalache *)
