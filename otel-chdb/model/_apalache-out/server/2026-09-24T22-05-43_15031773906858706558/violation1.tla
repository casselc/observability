---------------------------- MODULE counterexample ----------------------------

EXTENDS noRotationLock

(* Constant initialization state *)
ConstInit == TRUE

(* Initial state [_transition(0)] *)
State0 ==
  noRotationLock_edgePublish_centralCount
      = SetAsFun({ <<
          [bk |-> [batch |-> 1, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >> })
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
      = SetAsFun({ <<
          [bk |-> [batch |-> 1, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >> })
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
            payload |-> 1,
            phase |-> Variant("Encoded", [tag |-> "UNIT"])]},
        rotated |-> {},
        succeeded |-> {}]

(* State2 [_transition(4)] *)
State2 ==
  noRotationLock_edgePublish_centralCount
      = SetAsFun({ <<
          [bk |-> [batch |-> 1, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >> })
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
            payload |-> 1,
            phase |-> Variant("Encoded", [tag |-> "UNIT"])]},
        rotated |-> {[epoch |-> 1, gen |-> 1]},
        succeeded |-> {}]

(* State3 [_transition(5)] *)
State3 ==
  noRotationLock_edgePublish_centralCount
      = SetAsFun({ <<
          [bk |-> [batch |-> 1, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >> })
    /\ noRotationLock_edgePublish_dedupWindow = <<>>
    /\ noRotationLock_edgePublish_events = {"sealed"}
    /\ noRotationLock_edgePublish_ledger = {}
    /\ noRotationLock_edgePublish_ledgerContent = {}
    /\ noRotationLock_edgePublish_manifests = {}
    /\ noRotationLock_edgePublish_parquet = {}
    /\ noRotationLock_edgePublish_queue = { 1, 2 }
    /\ noRotationLock_edgePublish_seals
      = {[batches |-> {}, gk |-> [epoch |-> 1, gen |-> 1]]}
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
            payload |-> 1,
            phase |-> Variant("Encoded", [tag |-> "UNIT"])]},
        rotated |-> {},
        succeeded |-> {}]

(* State4 [_transition(1)] *)
State4 ==
  noRotationLock_edgePublish_centralCount
      = SetAsFun({ <<
          [bk |-> [batch |-> 1, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >> })
    /\ noRotationLock_edgePublish_dedupWindow = <<>>
    /\ noRotationLock_edgePublish_events = {"sealed"}
    /\ noRotationLock_edgePublish_ledger = {}
    /\ noRotationLock_edgePublish_ledgerContent = {}
    /\ noRotationLock_edgePublish_manifests = {}
    /\ noRotationLock_edgePublish_parquet = {}
    /\ noRotationLock_edgePublish_queue = { 1, 2 }
    /\ noRotationLock_edgePublish_seals
      = {[batches |-> {}, gk |-> [epoch |-> 1, gen |-> 1]]}
    /\ noRotationLock_edgePublish_tableRows
      = {[payload |-> 1,
        src |->
          [bk |-> [batch |-> 1, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]]]}
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
            payload |-> 1,
            phase |-> Variant("TableDone", [tag |-> "UNIT"])]},
        rotated |-> {},
        succeeded |-> {}]

(* State5 [_transition(2)] *)
State5 ==
  noRotationLock_edgePublish_centralCount
      = SetAsFun({ <<
          [bk |-> [batch |-> 1, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >> })
    /\ noRotationLock_edgePublish_dedupWindow = <<>>
    /\ noRotationLock_edgePublish_events = {"sealed"}
    /\ noRotationLock_edgePublish_ledger = {}
    /\ noRotationLock_edgePublish_ledgerContent = {}
    /\ noRotationLock_edgePublish_manifests = {}
    /\ noRotationLock_edgePublish_parquet
      = {[bk |-> [batch |-> 1, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]]}
    /\ noRotationLock_edgePublish_queue = { 1, 2 }
    /\ noRotationLock_edgePublish_seals
      = {[batches |-> {}, gk |-> [epoch |-> 1, gen |-> 1]]}
    /\ noRotationLock_edgePublish_tableRows
      = {[payload |-> 1,
        src |->
          [bk |-> [batch |-> 1, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]]]}
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
            payload |-> 1,
            phase |-> Variant("ParquetDone", [tag |-> "UNIT"])]},
        rotated |-> {},
        succeeded |-> {}]

(* State6 [_transition(3)] *)
State6 ==
  noRotationLock_edgePublish_centralCount
      = SetAsFun({ <<
          [bk |-> [batch |-> 1, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 1, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 2, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 2]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 1]], 0
        >>,
        <<
          [bk |-> [batch |-> 3, epoch |-> 2], gk |-> [epoch |-> 2, gen |-> 2]], 0
        >> })
    /\ noRotationLock_edgePublish_dedupWindow = <<>>
    /\ noRotationLock_edgePublish_events = {"sealed"}
    /\ noRotationLock_edgePublish_ledger = {}
    /\ noRotationLock_edgePublish_ledgerContent = {}
    /\ noRotationLock_edgePublish_manifests
      = {[bk |-> [batch |-> 1, epoch |-> 1],
        gk |-> [epoch |-> 1, gen |-> 1],
        payload |-> 1]}
    /\ noRotationLock_edgePublish_parquet
      = {[bk |-> [batch |-> 1, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]]}
    /\ noRotationLock_edgePublish_queue = {2}
    /\ noRotationLock_edgePublish_seals
      = {[batches |-> {}, gk |-> [epoch |-> 1, gen |-> 1]]}
    /\ noRotationLock_edgePublish_tableRows
      = {[payload |-> 1,
        src |->
          [bk |-> [batch |-> 1, epoch |-> 1], gk |-> [epoch |-> 1, gen |-> 1]]]}
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
  Skolem((\E noRotationLock_edgePublish_s_1795_2 \in noRotationLock_edgePublish_seals:
    ~(noRotationLock_edgePublish_s_1795_2["batches"]
      = {
        noRotationLock_edgePublish_m_1792_2["bk"]:
          noRotationLock_edgePublish_m_1792_2 \in
            {
              noRotationLock_edgePublish_m_1786_2 \in
                noRotationLock_edgePublish_manifests:
                noRotationLock_edgePublish_m_1786_2["gk"]
                  = noRotationLock_edgePublish_s_1795_2["gk"]
            }
      })))

================================================================================
(* Created by Apalache on Thu Sep 24 22:05:57 UTC 2026 *)
(* https://github.com/apalache-mc/apalache *)
