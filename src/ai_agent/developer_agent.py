import os
import pandas as pd
import ollama
import textwrap


class DeveloperAgent:
    """
    Automates PySpark code generation based on the LATEST RICE-prioritized backlog.
    Includes state management and chronological filtering.
    """

    def __init__(self, model="llama3.1"):
        self.model = model
        self.history_path = "docs/project_history.csv"

    def get_top_task(self):
        """Finds the best pending task from the most recent backlog run."""
        if not os.path.exists(self.history_path):
            print("[!] History file not found.")
            return None

        df = pd.read_csv(self.history_path)

        # 1. Convert Timestamp to datetime for reliable sorting
        df["Timestamp"] = pd.to_datetime(df["Timestamp"])

        # 2. Initialize Status column if missing
        if "Status" not in df.columns:
            df["Status"] = "Pending"

        # 3. Focus on the newest session (latest timestamp)
        latest_ts = df["Timestamp"].max()

        # 4. Sort: newest first, then highest Score
        df = df.sort_values(by=["Timestamp", "Score"], ascending=[False, False])

        # 5. Filter for the first pending task from the latest run
        # Note: We look at 'Pending' overall, but prioritize the latest entries
        pending_tasks = df[df["Status"] == "Pending"]

        if pending_tasks.empty:
            return None

        task_idx = pending_tasks.index[0]
        task_data = pending_tasks.iloc[0].to_dict()
        return task_data, task_idx

    def update_task_status(self, index, status="In Progress"):
        """Updates the status in CSV to prevent re-processing."""
        df = pd.read_csv(self.history_path)
        # Ensure column exists before update
        if "Status" not in df.columns:
            df["Status"] = "Pending"
        df.at[index, "Status"] = status
        df.to_csv(self.history_path, index=False)

    def generate_implementation(self, task, context_code):
        """Generates code block using local LLM."""

        system_prompt = (
            "You are a Senior Data Engineer specializing in PySpark. "
            "Write professional code. Max line length: 100 characters. "
            "Comments in English. Return ONLY the code block without markdown tags."
        )

        # Mapping 'Feature' as the main task description based on your CSV structure
        feature_desc = task.get("Feature", "New Transformation")

        user_prompt = (
            textwrap.dedent(
                """
            TASK: {feature}
            GOAL: Implement this as a PySpark transformation.
            
            EXISTING CONTEXT (transformer.py):
            {context}

            INSTRUCTION:
            Provide only the Python function or logic to be added.
        """
            )
            .strip()
            .format(feature=feature_desc, context=context_code)
        )

        response = ollama.generate(model=self.model, system=system_prompt, prompt=user_prompt)
        return response["response"]

    def run(self):
        """Orchestrates development process for the next prioritized task."""
        print("--- Developer Agent: Starting Code Generation ---")

        result = self.get_top_task()
        if not result:
            print("[!] No pending tasks found in the latest backlog.")
            return

        task, idx = result
        print(f"> Selected Task: {task['Feature']} (Score: {task['Score']})")
        print(f"> Timestamp: {task['Timestamp']}")

        with open("src/pyspark/transformer.py", "r", encoding="utf-8") as f:
            context = f.read()

        proposal = self.generate_implementation(task, context)

        output_path = "docs/developer_proposal.py"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(proposal)

        # Mark as In Progress so the next 'Play' takes the NEXT best task
        self.update_task_status(idx, "In Progress")

        print(f"[SUCCESS] Code generated at: {output_path}")
        print(f"[STATUS] Task marked as 'In Progress' in project_history.csv")


if __name__ == "__main__":
    agent = DeveloperAgent()
    agent.run()
