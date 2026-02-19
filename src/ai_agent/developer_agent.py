import os
import textwrap
import pandas as pd
import ollama

# Configuration aligned with BacklogOrchestrator output
MODEL_NAME = "llama3.1"
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
PROJECT_HISTORY_PATH = os.path.join(PROJECT_ROOT, "docs/project_history.csv")


def get_top_priority_task():
    """
    Selects the highest-scoring feature from the project history CSV.
    """
    if not os.path.exists(PROJECT_HISTORY_PATH):
        return None

    try:
        df = pd.read_csv(PROJECT_HISTORY_PATH)
        if df.empty:
            return None

        # Sort by Score descending and pick the top one
        top_task = df.sort_values(by="Score", ascending=False).iloc[0]
        return top_task
    except Exception as e:
        print(f"[!] Error reading history: {e}")
        return None


def generate_code_skeleton(feature_name):
    """
    Invokes local LLM to generate a professional PySpark code skeleton.
    """
    system_prompt = textwrap.dedent(
        """
        ROLE: Senior Data Engineer (PySpark Expert).
        STRICT RULES:
        1. Use ONLY standard PySpark 3.4.1 API. Do not invent methods.
        2. Format: Black-compliant (max-line-length = 100).
        3. Quality: PEP 8 compliant, professional docstrings, English comments.
        4. No educational chatter. Output only pure, production-ready code.
    """
    )

    user_prompt = f"TASK: Generate a PySpark module for the following feature: {feature_name}"

    try:
        response = ollama.chat(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        return response["message"]["content"]
    except Exception as e:
        return f"# Error generating code: {e}"


def main():
    print("[Developer Agent] Analyzing prioritized backlog...")
    task = get_top_priority_task()

    if task is None:
        print("[!] No prioritized features found. Run BacklogOrchestrator first.")
        return

    # Using 'Feature' and 'Score' as defined in your Orchestrator
    feature_name = task["Feature"]
    score = task["Score"]

    print(f"[Developer Agent] Selected Feature: {feature_name} (RICE Score: {score})")

    skeleton = generate_code_skeleton(feature_name)

    # Persistence
    output_dir = os.path.join(PROJECT_ROOT, "docs/generated_code")
    os.makedirs(output_dir, exist_ok=True)

    safe_name = feature_name.lower().replace(" ", "_")[:30]
    output_path = os.path.join(output_dir, f"feat_{safe_name}.py")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(skeleton)

    print(f"[+] Technical skeleton generated: {output_path}")


if __name__ == "__main__":
    main()
