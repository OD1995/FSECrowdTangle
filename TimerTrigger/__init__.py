import datetime
import logging
import azure.durable_functions as df
import azure.functions as func


async def main(mytimer: func.TimerRequest, starter: str) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    client = df.DurableOrchestrationClient(starter)
    ## Start orchestrator
    instance_id = await client.start_new(
        orchestration_function_name="Orchestrator",
        instance_id=None
    )
    logging.info(f"Started orchestration with ID = '{instance_id}'.")

    logging.info('Python timer trigger function ran at %s', utc_timestamp)