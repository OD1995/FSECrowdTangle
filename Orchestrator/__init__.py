# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "Hello")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

from logging import info
import azure.functions as func
import azure.durable_functions as df

def orchestrator_function(context: df.DurableOrchestrationContext):
    info('The orchestration has started')
    result1 = yield context.call_activity(
        name='ScrapeCrowdTanglePart1',
        input_='name_str1'
    )
    info('Part1 done')
    result2 = yield context.call_activity(
        name='ScrapeCrowdTanglePart2',
        input_='name_str2'
    )
    info('Part2 done')
    result3 = yield context.call_activity(
        name='ScrapeCrowdTanglePart3',
        input_='name_str3'
    )
    info('Part3 done')
    info('Orchestration finished')
    return [result1,result2,result3]

main = df.Orchestrator.create(orchestrator_function)