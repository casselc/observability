// -*- mode: Bluespec; -*-
/**
 * openInstances: wider instances of the consumer model for the open-ended
 * nightly (`model-open`, ci/model-open.sh; VERIFICATION.md §7 P1). Each is
 * a design instance of s3InlineConsumer.qnt with a longer calendar, a
 * wider copy horizon and a longer lease than the per-push and `model` jobs
 * can afford to sample: three days (HORIZON 2, as the design requires
 * HORIZON >= MAX_DAY) and TTL 9 (renewals due every 3 ticks). The log's
 * domain (two payloads, two epochs, three slots, two workers) is fixed in
 * s3InlineConsumer itself.
 */
// The design in the full hostile environment, over three days.
module wideDesign {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = true, SERIES = true, TTL = 9, SLACK = 1, RELEASE_GUARD = true, HORIZON = 2, WALL_RANGE = false,
    MAX_DAY = 2, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = true,
    SLOW_OBS = false, OBSERVE_AT_REQUEST = false, SCAN = false, SCAN_BY_TIME = true, CAS_AMBIG = false, OWN412_TAKEOVER = false,
    LATE_CAS = false, CKPT_REFRESH = true, LEASE_REFRESH = true,
    TAKE_AMBIG = false, TAKE_REFRESH = true).* from "./s3InlineConsumer"
}
// The design with every duration (slow LISTs, HEADs that cost time,
// ambiguous and late lease and checkpoint writes), over three days.
module wideSlow {
  import s3InlineConsumer(TIME_BOUND = true, VERIFY = true, GC_TOMBS = false, ANNOUNCE_EARLY = false,
    WRITER_FAULTS = true, SERIES = false, TTL = 9, SLACK = 1, RELEASE_GUARD = true, HORIZON = 2, WALL_RANGE = false,
    MAX_DAY = 2, GC_KEEP = true, ERROR_SETTLES = false,
    RESTAMP = false, RESENDS = true,
    SLOW_OBS = true, OBSERVE_AT_REQUEST = false, SCAN = true, SCAN_BY_TIME = true, CAS_AMBIG = true, OWN412_TAKEOVER = false,
    LATE_CAS = true, CKPT_REFRESH = true, LEASE_REFRESH = true,
    TAKE_AMBIG = true, TAKE_REFRESH = true).* from "./s3InlineConsumer"
}
