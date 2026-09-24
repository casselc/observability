---------------------------- MODULE counterexample ----------------------------

EXTENDS lifetime0

(* Constant initialization state *)
ConstInit == TRUE

(* Initial state [_transition(0)] *)
State0 ==
  lifetime0_partLifetime_acked = FALSE
    /\ lifetime0_partLifetime_badRead = FALSE
    /\ lifetime0_partLifetime_events = {}
    /\ lifetime0_partLifetime_gcDone = FALSE
    /\ lifetime0_partLifetime_now = 0
    /\ lifetime0_partLifetime_parts
      = SetAsFun({ <<
          1, [covers |-> {},
            since |-> 0,
            st |-> Variant("Absent", [tag |-> "UNIT"])]
        >>,
        <<
          2, [covers |-> {},
            since |-> 0,
            st |-> Variant("Absent", [tag |-> "UNIT"])]
        >>,
        <<
          3, [covers |-> {},
            since |-> 0,
            st |-> Variant("Absent", [tag |-> "UNIT"])]
        >> })
    /\ lifetime0_partLifetime_readers
      = SetAsFun({<<
        1, [attached |-> FALSE,
          q |-> [parts |-> {}, read |-> {}, running |-> FALSE, started |-> 0],
          refreshedAt |-> 0,
          refreshing |-> FALSE,
          view |-> {}]
      >>})
    /\ lifetime0_partLifetime_writerUp = TRUE

(* State1 [_transition(0)] *)
State1 ==
  lifetime0_partLifetime_acked = FALSE
    /\ lifetime0_partLifetime_badRead = FALSE
    /\ lifetime0_partLifetime_events = {"insert"}
    /\ lifetime0_partLifetime_gcDone = FALSE
    /\ lifetime0_partLifetime_now = 0
    /\ lifetime0_partLifetime_parts
      = SetAsFun({ <<
          1, [covers |-> {},
            since |-> 0,
            st |-> Variant("Active", [tag |-> "UNIT"])]
        >>,
        <<
          2, [covers |-> {},
            since |-> 0,
            st |-> Variant("Absent", [tag |-> "UNIT"])]
        >>,
        <<
          3, [covers |-> {},
            since |-> 0,
            st |-> Variant("Absent", [tag |-> "UNIT"])]
        >> })
    /\ lifetime0_partLifetime_readers
      = SetAsFun({<<
        1, [attached |-> FALSE,
          q |-> [parts |-> {}, read |-> {}, running |-> FALSE, started |-> 0],
          refreshedAt |-> 0,
          refreshing |-> FALSE,
          view |-> {}]
      >>})
    /\ lifetime0_partLifetime_writerUp = TRUE

(* State2 [_transition(4)] *)
State2 ==
  lifetime0_partLifetime_acked = FALSE
    /\ lifetime0_partLifetime_badRead = FALSE
    /\ lifetime0_partLifetime_events = {"insert"}
    /\ lifetime0_partLifetime_gcDone = FALSE
    /\ lifetime0_partLifetime_now = 0
    /\ lifetime0_partLifetime_parts
      = SetAsFun({ <<
          1, [covers |-> {},
            since |-> 0,
            st |-> Variant("Active", [tag |-> "UNIT"])]
        >>,
        <<
          2, [covers |-> {},
            since |-> 0,
            st |-> Variant("Absent", [tag |-> "UNIT"])]
        >>,
        <<
          3, [covers |-> {},
            since |-> 0,
            st |-> Variant("Absent", [tag |-> "UNIT"])]
        >> })
    /\ lifetime0_partLifetime_readers
      = SetAsFun({<<
        1, [attached |-> TRUE,
          q |-> [parts |-> {}, read |-> {}, running |-> FALSE, started |-> 0],
          refreshedAt |-> 0,
          refreshing |-> TRUE,
          view |-> {1}]
      >>})
    /\ lifetime0_partLifetime_writerUp = TRUE

(* State3 [_transition(0)] *)
State3 ==
  lifetime0_partLifetime_acked = FALSE
    /\ lifetime0_partLifetime_badRead = FALSE
    /\ lifetime0_partLifetime_events = {"insert"}
    /\ lifetime0_partLifetime_gcDone = FALSE
    /\ lifetime0_partLifetime_now = 0
    /\ lifetime0_partLifetime_parts
      = SetAsFun({ <<
          1, [covers |-> {},
            since |-> 0,
            st |-> Variant("Active", [tag |-> "UNIT"])]
        >>,
        <<
          2, [covers |-> {},
            since |-> 0,
            st |-> Variant("Active", [tag |-> "UNIT"])]
        >>,
        <<
          3, [covers |-> {},
            since |-> 0,
            st |-> Variant("Absent", [tag |-> "UNIT"])]
        >> })
    /\ lifetime0_partLifetime_readers
      = SetAsFun({<<
        1, [attached |-> TRUE,
          q |-> [parts |-> {}, read |-> {}, running |-> FALSE, started |-> 0],
          refreshedAt |-> 0,
          refreshing |-> TRUE,
          view |-> {1}]
      >>})
    /\ lifetime0_partLifetime_writerUp = TRUE

(* State4 [_transition(1)] *)
State4 ==
  lifetime0_partLifetime_acked = FALSE
    /\ lifetime0_partLifetime_badRead = FALSE
    /\ lifetime0_partLifetime_events = { "insert", "merge" }
    /\ lifetime0_partLifetime_gcDone = FALSE
    /\ lifetime0_partLifetime_now = 0
    /\ lifetime0_partLifetime_parts
      = SetAsFun({ <<
          1, [covers |-> {},
            since |-> 0,
            st |-> Variant("Outdated", [tag |-> "UNIT"])]
        >>,
        <<
          2, [covers |-> {},
            since |-> 0,
            st |-> Variant("Outdated", [tag |-> "UNIT"])]
        >>,
        <<
          3, [covers |-> { 1, 2 },
            since |-> 0,
            st |-> Variant("Active", [tag |-> "UNIT"])]
        >> })
    /\ lifetime0_partLifetime_readers
      = SetAsFun({<<
        1, [attached |-> TRUE,
          q |-> [parts |-> {}, read |-> {}, running |-> FALSE, started |-> 0],
          refreshedAt |-> 0,
          refreshing |-> TRUE,
          view |-> {1}]
      >>})
    /\ lifetime0_partLifetime_writerUp = TRUE

(* State5 [_transition(2)] *)
State5 ==
  lifetime0_partLifetime_acked = FALSE
    /\ lifetime0_partLifetime_badRead = FALSE
    /\ lifetime0_partLifetime_events = { "cleanup", "insert", "merge" }
    /\ lifetime0_partLifetime_gcDone = FALSE
    /\ lifetime0_partLifetime_now = 0
    /\ lifetime0_partLifetime_parts
      = SetAsFun({ <<
          1, [covers |-> {},
            since |-> 0,
            st |-> Variant("Deleted", [tag |-> "UNIT"])]
        >>,
        <<
          2, [covers |-> {},
            since |-> 0,
            st |-> Variant("Outdated", [tag |-> "UNIT"])]
        >>,
        <<
          3, [covers |-> { 1, 2 },
            since |-> 0,
            st |-> Variant("Active", [tag |-> "UNIT"])]
        >> })
    /\ lifetime0_partLifetime_readers
      = SetAsFun({<<
        1, [attached |-> TRUE,
          q |-> [parts |-> {}, read |-> {}, running |-> FALSE, started |-> 0],
          refreshedAt |-> 0,
          refreshing |-> TRUE,
          view |-> {1}]
      >>})
    /\ lifetime0_partLifetime_writerUp = TRUE

(* State6 [_transition(7)] *)
State6 ==
  lifetime0_partLifetime_acked = FALSE
    /\ lifetime0_partLifetime_badRead = FALSE
    /\ lifetime0_partLifetime_events = { "cleanup", "insert", "merge" }
    /\ lifetime0_partLifetime_gcDone = FALSE
    /\ lifetime0_partLifetime_now = 0
    /\ lifetime0_partLifetime_parts
      = SetAsFun({ <<
          1, [covers |-> {},
            since |-> 0,
            st |-> Variant("Deleted", [tag |-> "UNIT"])]
        >>,
        <<
          2, [covers |-> {},
            since |-> 0,
            st |-> Variant("Outdated", [tag |-> "UNIT"])]
        >>,
        <<
          3, [covers |-> { 1, 2 },
            since |-> 0,
            st |-> Variant("Active", [tag |-> "UNIT"])]
        >> })
    /\ lifetime0_partLifetime_readers
      = SetAsFun({<<
        1, [attached |-> TRUE,
          q |-> [parts |-> {1}, read |-> {}, running |-> TRUE, started |-> 0],
          refreshedAt |-> 0,
          refreshing |-> TRUE,
          view |-> {1}]
      >>})
    /\ lifetime0_partLifetime_writerUp = TRUE

(* State7 [_transition(8)] *)
State7 ==
  lifetime0_partLifetime_acked = FALSE
    /\ lifetime0_partLifetime_badRead = TRUE
    /\ lifetime0_partLifetime_events
      = { "badRead", "cleanup", "insert", "merge" }
    /\ lifetime0_partLifetime_gcDone = FALSE
    /\ lifetime0_partLifetime_now = 0
    /\ lifetime0_partLifetime_parts
      = SetAsFun({ <<
          1, [covers |-> {},
            since |-> 0,
            st |-> Variant("Deleted", [tag |-> "UNIT"])]
        >>,
        <<
          2, [covers |-> {},
            since |-> 0,
            st |-> Variant("Outdated", [tag |-> "UNIT"])]
        >>,
        <<
          3, [covers |-> { 1, 2 },
            since |-> 0,
            st |-> Variant("Active", [tag |-> "UNIT"])]
        >> })
    /\ lifetime0_partLifetime_readers
      = SetAsFun({<<
        1, [attached |-> TRUE,
          q |-> [parts |-> {}, read |-> {}, running |-> FALSE, started |-> 0],
          refreshedAt |-> 0,
          refreshing |-> TRUE,
          view |-> {1}]
      >>})
    /\ lifetime0_partLifetime_writerUp = TRUE

(* The following formula holds true in the last state and violates the invariant *)
InvariantViolation == lifetime0_partLifetime_badRead

================================================================================
(* Created by Apalache on Thu Sep 24 21:51:39 UTC 2026 *)
(* https://github.com/apalache-mc/apalache *)
