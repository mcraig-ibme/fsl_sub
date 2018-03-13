#!/usr/bin/env python
copro_capability = 1
cpconf = {}
cpconf['class_types'] = {'V': {'resource': 'v100', 'capability': 3}, 'K': {'resource': 'k80', 'capability': 1}, 'P': {'resource': 'p100', 'capability': 2 }, }
alist = [a['resource'] for a in
         cpconf['class_types'].values() if
         a['capability'] >= copro_capability]
base_list = [a for a in cpconf['class_types'].keys() if cpconf['class_types'][a]['capability'] >= copro_capability]
print(base_list)
new_base_list = sorted(base_list, key=lambda x: cpconf['class_types'][x]['capability'])
print(new_base_list)
resources = [cpconf['class_types'][a]['resource'] for a in new_base_list]
print(resources)

queues = {
    'a': {'group': 0, 'priority': 1},
    'b': {'group': 0, 'priority': 2},
    'c': {'group': 0, 'priority': 3},
    'A': {'group': 1, 'priority': 1},
    'B': {'group': 1, 'priority': 2},
    'C': {'group': 1, 'priority': 3},
}

queue_list = ['a', 'A', 'C', 'b', 'B', 'c', ]
print("Unsorted")
print(queue_list)
queue_list.sort(key=lambda x: queues[x]['priority'], reverse=True)
print("Sort by priority")
print(queue_list)
queue_list.sort(key=lambda x: queues[x]['group'])
print("Sort by group")
print(queue_list)

queues = {
  't.q': {
    'parallel_envs': [ 'specialpe', ],
  }
}

ll_env = 'specialpe'

queue_list = ['t.q']
queue_list = [
    q for q in queue_list if 'parallel_envs' in queues[q]
]
print(queue_list)
queue_list = [
    q for q in queue_list if ll_env in queues[q]['parallel_envs']
]
print(queue_list)
queue_list = [
    q for q in queue_list if 'parallel_envs' in queues[q]
    and ll_env in queues[q]['parallel_envs']
]
print(queue_list)