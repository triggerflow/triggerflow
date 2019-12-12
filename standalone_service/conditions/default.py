def condition_true(context, event):
    return True


def condition_ibm_cf_join(context, event):
    trigger_source_id = event['triggersource']
    trigger_source_context = context['triggers'][trigger_source_id]['context']

    if trigger_source_context['counter'] < trigger_source_context['total_activations']:
        trigger_source_context['counter'] += 1

    if trigger_source_context['counter'] == trigger_source_context['total_activations']:
        map_join = True
    else:
        return False

    if len(context['depends_on_events']) > 1:
        deps = []
        for dependency in context['depends_on_events']:
            if dependency in context['events']:
                prev_event = context['events'][dependency][0]
                trigger_id = prev_event['triggersource']
                dependency_trigger_context = context['triggers'][trigger_id]['context']
                deps.append(dependency_trigger_context['total_activations'] == dependency_trigger_context['counter'])
        branch_join = all(deps)
    else:
        branch_join = True

    return map_join and branch_join
