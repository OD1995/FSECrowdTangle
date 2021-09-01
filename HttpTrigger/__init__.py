import logging

import azure.durable_functions as df
import azure.functions as func


async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)
    ## Start orchestrator
    instance_id = await client.start_new(
        orchestration_function_name="Orchestrator",
        instance_id=None
    )

    logging.info(f"Started orchestration with ID = '{instance_id}'.")

    return client.create_check_status_response(req, instance_id)

