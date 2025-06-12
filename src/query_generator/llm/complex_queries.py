import asyncio

from ollama import AsyncClient
from tqdm.asyncio import tqdm

from query_generator.utils.params import (
  ComplexQueryGenerationParametersEndpoint,
)


async def query_llm_async(client: AsyncClient, prompt: str, model: str) -> str:
  """Send a single request to the LLM and return its response."""
  response = await client.chat(
    model=model, messages=[{"role": "user", "content": prompt}], stream=False
  )
  response_str = response.message.content
  if not response_str:
    return ""
  return response_str


async def limited_query(
  prompt: str, client: AsyncClient, model: str, semaphore: asyncio.Semaphore
) -> tuple[str, str]:
  """
  Wrapper that limits concurrency using a semaphore.

  Returns:
      A tuple of (prompt, response_text).
  """
  async with semaphore:
    response = await query_llm_async(client, prompt, model)
    return prompt, response


async def create_complex_queries(
  params: ComplexQueryGenerationParametersEndpoint,
) -> None:
  """Generate complex queries in parallel with controlled concurrency."""
  client = AsyncClient()
  countries = ["Brazil", "Colombia", "Peru", "Argentina"]
  prompts = [f"What is the capital city of {country}?" for country in countries]

  # Determine concurrency limit
  max_concurrent = getattr(params, "max_concurrent_requests", 5) or 5
  semaphore = asyncio.Semaphore(max_concurrent)

  # Schedule tasks with concurrency control
  tasks = [
    asyncio.create_task(
      limited_query(prompt, client, params.llm_model, semaphore)
    )
    for prompt in prompts
  ]

  results: list[tuple[str, str]] = []
  for future in tqdm(
    asyncio.as_completed(tasks), total=len(tasks), desc="Generating queries"
  ):
    prompt, response = await future
    results.append((prompt, response))

  # Output results
  for prompt, response in results:
    print(f"Prompt: {prompt}\nResponse: {response}\n")


def run_create_complex_queries(
  params: ComplexQueryGenerationParametersEndpoint,
) -> None:
  """
  Synchronous entry point to trigger the async generator.
  """
  asyncio.run(create_complex_queries(params))
