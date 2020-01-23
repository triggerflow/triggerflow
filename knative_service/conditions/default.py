def condition_true(context, event):
    return True


def condition_ibm_cf_join(context, event):
    if 'counter' not in context:
        context['counter'] = 1
    else:
        context['counter'] += 1

    return context['counter'] == context['total_activations']
