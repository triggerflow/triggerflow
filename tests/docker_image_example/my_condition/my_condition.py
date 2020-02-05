
def counter_threshold(event, context):
    if 'counter' not in context:
        context['counter'] = 1

    return context['counter'] == context['threshold']
