import json


def inspect_document(body):
    value = json.loads(body)
    if not isinstance(value, dict) or not isinstance(value.get('bootId'), str):
        raise ValueError('缺少启动期')
    samples = value.get('samples', [])
    if not isinstance(samples, list):
        raise ValueError('样本不是数组')
    ticks = []
    for sample in samples:
        tick = sample.get('tick')
        if not isinstance(tick, int) or isinstance(tick, bool) or tick < 0:
            raise ValueError('tick 必须是非负整数')
        ticks.append(tick)
    return {'bootId': value['bootId'], 'sample_count': len(samples),
            'minimum_tick': min(ticks) if ticks else None,
            'maximum_tick': max(ticks) if ticks else None,
            'truncated': value.get('truncated', False), 'revision': value.get('revision')}
