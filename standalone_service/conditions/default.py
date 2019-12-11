def condition_true(context, event):
    return True


def condition_join(context, event):
    context['counter'] += 1

    if context['counter'] == context['activations_done']:
        map_join = True
    else:
        return False

    if len(context['depends_on_events']) > 1:
        dependencies_checked = [event for event in context['depends_on_events'] if event in context['events']]
        branch_join = set(dependencies_checked) == set(context['depends_on_event'])
    else:
        branch_join = True

    return map_join and branch_join
