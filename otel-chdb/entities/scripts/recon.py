"""The view's ResourceAttributes expression: the covered set rebuilt from the
normalized catalog dictionaries (../sql/dictionaries.sql), keys sorted,
overlaid with the row's ResourceResidual."""


def covered(cat, rid="resource_id"):
    pk = f"dictGet('{cat}.d_res', 'pod_key', {rid})"
    ct = f"dictGet('{cat}.d_res', 'ct', {rid})"
    pod = lambda a: f"dictGet('{cat}.d_pod', '{a}', {pk})"
    lvl = lambda d, key: f"JSONExtract(dictGet('{cat}.{d}', 'attrs', {pod(key)}), 'Map(String, String)')"
    return (f"if(dictHas('{cat}.d_res', {rid}), mapConcat({lvl('d_cluster', 'cluster_key')}, {lvl('d_node', 'node_key')}, {lvl('d_ns', 'ns_key')}, "
            f"{lvl('d_wl', 'wl_key')}, map('k8s.pod.name', {pod('name')}, 'k8s.pod.uid', toString({pod('uid')}), "
            f"'k8s.pod.start_time', formatDateTime({pod('start')}, '%Y-%m-%dT%H:%i:%SZ', 'UTC')), JSONExtract({pod('extra')}, 'Map(String, String)'), "
            f"JSONExtract(dictGet('{cat}.d_wl', 'containers', {pod('wl_key')}), 'Array(Map(String, String))')[{ct}]), CAST(map(), 'Map(String, String)'))")


def resource_attributes(cat, rid="resource_id", residual="ResourceResidual"):
    return (f"CAST(mapSort(mapUpdate({covered(cat, rid)}, CAST({residual}, 'Map(String, String)'))), "
            f"'Map(LowCardinality(String), String)')")
