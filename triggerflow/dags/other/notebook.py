import graphviz


def display_graph(dag):
    graph = graphviz.Digraph()

    for task in dag.tasks:
        graph.node(task.task_id)

    for task in dag.tasks:
        for downstream_dep in task.downstream_relatives:
            graph.edge(task.task_id, downstream_dep.task_id)

    return graph
