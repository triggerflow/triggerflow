from jsonpath_ng import parse, jsonpath
from dateutil import parser

lambdas_comparisons = {
    'BooleanEquals': lambda a, b: a == b,
    'NumericEquals': lambda a, b: a == b,
    'NumericGreaterThan': lambda a, b: a > b,
    'NumericGreaterThanEquals': lambda a, b: a >= b,
    'NumericLessThan': lambda a, b: a < b,
    'NumericLessThanEquals': lambda a, b: a <= b,
    'StringEquals': lambda a, b: a == b,
    'StringGreaterThan': lambda a, b: a > b,
    'StringGreaterThanEquals': lambda a, b: a >= b,
    'StringLessThan': lambda a, b: a < b,
    'StringLessThanEquals': lambda a, b: a <= b,
    'TimestampEquals': lambda a, b: parser.parse(a) == parser.parse(b),
    'TimestampGreaterThan': lambda a, b: parser.parse(a) > parser.parse(a),
    'TimestampGreaterThanEquals': lambda a, b: parser.parse(a) >= parser.parse(a),
    'TimestampLessThan': lambda a, b: parser.parse(a) < parser.parse(a),
    'TimestampLessThanEquals': lambda a, b: parser.parse(a) < parser.parse(a)
}


def condition_aws_asf_condition(context, event):
    if 'Condition' in context:
        condition = context['Condition']
        if 'Not' in condition:
            eval_condition = not __evaluate_condition(condition, context['global_context'])
        elif 'And' in condition:
            eval_condition = True
            for cond in condition['And']:
                if not __evaluate_condition(cond, context['global_context']):
                    eval_condition = False
                    break
        elif 'Or' in condition:
            eval_condition = False
            for cond in condition['Or']:
                if __evaluate_condition(cond, context['global_context']):
                    eval_condition = True
                    break
        else:
            eval_condition = __evaluate_condition(condition, event['data'])
        return eval_condition
    else:
        return True


def condition_aws_asf_join_statemachine(context, event):
    if 'join_multiple' in context:
        if 'counter' in context:
            context['counter'] += 1
        else:
            context['counter'] = 1

        return context['counter'] >= context['join_multiple']
    else:
        return True


def __evaluate_condition(condition, data):
    exp = parse(condition['Variable'])
    matches = [match.value for match in exp.find(data)]

    results = []
    for match in matches:
        intersect = set(condition).intersection(set(lambdas_comparisons)).pop()
        f = lambdas_comparisons[intersect]
        res = f(match, condition[intersect])
        results.append(res)

    return all(results)
