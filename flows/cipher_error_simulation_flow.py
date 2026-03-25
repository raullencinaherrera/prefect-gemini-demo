import random
import string
from typing import List

from prefect import flow, task
from prefect.context import get_run_context
from prefect.utilities.console import get_console

# Custom exception for ciphering errors
class CipherError(Exception):
    """Custom exception for errors during ciphering simulation."""
    pass

@task
def _process_char(
    char: str,
    p_fail_char_task: float,
    p_corrupt_char_output: float
) -> str:
    """
    Simulates processing a single character, with chances to fail the task
    or corrupt its output.

    Args:
        char: The character to process.
        p_fail_char_task: Probability (0.0-1.0) of this character task failing.
        p_corrupt_char_output: Probability (0.0-1.0) of this character task
                               corrupting its output instead of failing.

    Returns:
        The processed (or corrupted) character.

    Raises:
        CipherError: If the task simulates a failure.
    """
    task_run_name = get_run_context().task_run.name
    console = get_console()

    # Simulate task failure for the character
    if random.random() < p_fail_char_task:
        console.log(f"[bold red]Task '{task_run_name}'[/bold red]: Simulating character task failure for '{char}'")
        raise CipherError(f"Failed to process character '{char}' due to simulated system error.")

    # Simulate output corruption for the character
    if random.random() < p_corrupt_char_output:
        corrupted_char = random.choice(string.punctuation + string.digits + string.ascii_letters)
        console.log(f"[bold yellow]Task '{task_run_name}'[/bold yellow]: Corrupting char '{char}' to '{corrupted_char}'")
        return corrupted_char

    # Simple "ciphering" if no error
    ciphered_char = char.upper() # Example simple transformation
    console.log(f"Task '{task_run_name}': Processed char '{char}' to '{ciphered_char}'")
    return ciphered_char

@task
def _process_text_block(
    text_block: str,
    p_fail_block_task: float,
    p_fail_char_task: float,
    p_corrupt_char_output: float
) -> str:
    """
    Simulates ciphering a block of text. Can fail the entire block task
    or process characters, which themselves can fail or corrupt.

    Args:
        text_block: The text block to process.
        p_fail_block_task: Probability (0.0-1.0) of this entire block task failing.
        p_fail_char_task: Probability (0.0-1.0) of an individual character
                          sub-task failing.
        p_corrupt_char_output: Probability (0.0-1.0) of an individual character
                               sub-task corrupting its output.

    Returns:
        The processed (or partially corrupted) text block.

    Raises:
        CipherError: If the block task itself simulates a failure, or if any
                     character sub-task fails.
    """
    block_id = text_block[:10] + "..." if len(text_block) > 10 else text_block
    task_run_name = get_run_context().task_run.name
    console = get_console()

    # Simulate entire block task failure
    if random.random() < p_fail_block_task:
        console.log(f"[bold red]Task '{task_run_name}'[/bold red]: Simulating block task failure for '{block_id}'")
        raise CipherError(f"Entire block '{block_id}' failed due to simulated network error.")

    console.log(f"Task '{task_run_name}': Starting to process block '{block_id}' ({len(text_block)} chars)")

    # Process each character in the block using mapped sub-tasks
    ciphered_chars_futures = [
        _process_char.submit(char, p_fail_char_task, p_corrupt_char_output)
        for char in text_block
    ]

    processed_chars = []
    # Collect results. If any character sub-task fails, its exception will be
    # re-raised by .result(), causing this parent block task to fail.
    for future in ciphered_chars_futures:
        try:
            processed_chars.append(future.result())
        except CipherError as e:
            # Re-raise to fail the parent block task if a char task failed.
            # This demonstrates cascading failures.
            console.log(f"[bold red]Task '{task_run_name}'[/bold red]: A character task failed: {e}")
            raise # Fail the _process_text_block task

    result_block = "".join(processed_chars)
    console.log(f"Task '{task_run_name}': Finished block '{block_id}', result length: {len(result_block)}")
    return result_block

@flow(name="Simulate Ciphering Errors")
def simulate_cipher_errors_flow(
    source_code: str = "import os\n\ndef main():\n    print('Hello, Prefect!')\n",
    block_size: int = 20,
    p_fail_block_task: float = 0.1,  # Probability an entire block processing task fails
    p_fail_char_task: float = 0.05,  # Probability a character sub-task fails
    p_corrupt_char_output: float = 0.15 # Probability a character sub-task corrupts output
) -> List[str]:
    """
    A flow to simulate errors during a 'ciphering' process of a code snippet.
    This is useful for testing Prefect's error handling, retries, and state management.

    Args:
        source_code: The input code string to 'cipher'.
        block_size: The maximum number of characters per processing block.
        p_fail_block_task: Probability (0.0-1.0) that an entire text block task fails.
        p_fail_char_task: Probability (0.0-1.0) that an individual character
                          processing sub-task fails.
        p_corrupt_char_output: Probability (0.0-1.0) that an individual character
                               processing sub-task corrupts its output instead of failing.

    Returns:
        A list of processed (or placeholder for failed) text blocks.
    """
    console = get_console()
    console.log(f"[bold blue]Starting flow 'Simulate Ciphering Errors'[/bold blue]")
    console.log(f"Input code length: {len(source_code)} characters")
    console.log(f"Block size: {block_size}")
    console.log(f"Block task failure rate: {p_fail_block_task*100:.1f}% ")
    console.log(f"Character task failure rate: {p_fail_char_task*100:.1f}% ")
    console.log(f"Character output corruption rate: {p_corrupt_char_output*100:.1f}% ")

    # Split the source code into blocks
    text_blocks = [
        source_code[i:i + block_size]
        for i in range(0, len(source_code), block_size)
    ]
    console.log(f"Split into {len(text_blocks)} blocks.")

    # Map the block processing task over all blocks
    processed_block_futures = _process_text_block.map(
        text_blocks,
        p_fail_block_task=p_fail_block_task,
        p_fail_char_task=p_fail_char_task,
        p_corrupt_char_output=p_corrupt_char_output
    )

    # Collect the results, handling potential failures of mapped tasks.
    final_ciphered_blocks = []
    for future in processed_block_futures:
        try:
            final_ciphered_blocks.append(future.result())
        except CipherError:
            # If a block task failed, its result won't be available.
            # We append a placeholder to indicate a failed block without stopping the flow.
            console.log("[bold red]Flow[/bold red]: A text block task failed and its result could not be retrieved.")
            final_ciphered_blocks.append("[BLOCK_FAILED]")
        except Exception as e:
            # Catch any other unexpected errors during result collection
            console.log(f"[bold red]Flow[/bold red]: An unexpected error occurred while collecting block result: {e}")
            final_ciphered_blocks.append("[UNKNOWN_ERROR]")

    console.log(f"[bold blue]Flow finished. Collected {len(final_ciphered_blocks)} results (including failures).[/bold blue]")
    final_result_str = "".join(final_ciphered_blocks)
    console.log(f"Combined Output (first 100 chars): {final_result_str[:100]}...")
    return final_ciphered_blocks

if __name__ == "__main__":
    # Example usage:
    # Run with default parameters to observe mixed outcomes (some failures, some corruptions)
    simulate_cipher_errors_flow()

    # Example: Run with high failure rates to ensure many failures
    # simulate_cipher_errors_flow(
    #     p_fail_block_task=0.8,
    #     p_fail_char_task=0.3,
    #     p_corrupt_char_output=0.0
    # )

    # Example: Run with high corruption, low failure
    # simulate_cipher_errors_flow(
    #     p_fail_block_task=0.0,
    #     p_fail_char_task=0.0,
    #     p_corrupt_char_output=0.9
    # )

    # Example: Run with no errors to see successful "ciphering" (all uppercase)
    # simulate_cipher_errors_flow(
    #     p_fail_block_task=0.0,
    #     p_fail_char_task=0.0,
    #     p_corrupt_char_output=0.0
    # )
