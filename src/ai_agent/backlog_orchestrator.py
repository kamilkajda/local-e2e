import csv
import os
import re
import textwrap
import time
from datetime import datetime

import ollama

# --- SYSTEM PATH CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "../../"))
MODEL_NAME = os.getenv("OLLAMA_MODEL", "llama3.1")


class ProjectReaderAgent:
    """
    Handles technical context extraction including source code,
    infrastructure scripts (Azurite), and redacted configuration files.
    """

    def __init__(self, model=MODEL_NAME):
        self.model = model
        self.source_dir = os.path.join(PROJECT_ROOT, "src")
        self.infra_ext = (".json", ".yaml", ".yml", ".sh", ".ps1", ".env")

    def _scrub_secrets(self, text):
        """
        Redacts high-entropy strings and common secret patterns (API keys, passwords).
        """
        patterns = [
            r'(?i)(api[_-]?key|password|secret|token|account[_-]?key)\s*[:=]\s*["\'][^"\']+["\']',
            r'(?i)(["\']connection[_-]?string["\'])\s*[:=]\s*["\'][^"\']+["\']',
        ]
        scrubbed = text
        for pattern in patterns:
            # Removed trailing comma from replacement to keep JSON/Config integrity
            scrubbed = re.sub(pattern, r'\1: "**** REDACTED_FOR_SECURITY ****"', scrubbed)
        return scrubbed

    def get_context(self):
        start_time = time.perf_counter()
        structure = []
        config_data = []

        search_paths = [self.source_dir, PROJECT_ROOT]

        for path in search_paths:
            if not os.path.exists(path):
                continue
            for root, _, files in os.walk(path):
                if any(x in root for x in ["venv", ".git", "__pycache__"]):
                    continue

                for f in files:
                    rel_path = os.path.relpath(os.path.join(root, f), PROJECT_ROOT)
                    structure.append(rel_path)

                    if f.endswith(self.infra_ext) or "docker-compose" in f:
                        try:
                            with open(os.path.join(root, f), "r", encoding="utf-8") as cf:
                                content = cf.read()[:2000]
                                clean_content = self._scrub_secrets(content)
                                config_data.append(f"--- FILE: {rel_path} ---\n{clean_content}")
                        except Exception:
                            continue

        readme_path = os.path.join(PROJECT_ROOT, "README.md")
        readme_content = ""
        if os.path.exists(readme_path):
            with open(readme_path, "r", encoding="utf-8") as r:
                readme_content = r.read()[:1500]

        prompt = textwrap.dedent(
            f"""
            Context Analysis Request:
            FILE_STRUCTURE: {structure}
            CONFIG_AND_INFRA_DETAILS:
            {config_data}
            PROJECT_README:
            {readme_content}
            Objective: Assess the PySpark-to-Azurite workflow based on available configs.
        """
        ).strip()

        print(f"[*] Analyst Agent is scanning infra & code using {self.model}...")
        response = ollama.chat(model=self.model, messages=[{"role": "user", "content": prompt}])

        end_time = time.perf_counter()
        print(f"[Performance] Analyst Agent processing time: {end_time - start_time:.2f}s")
        return response["message"]["content"]


class ProductOwnerAgent:
    """
    Generates prioritized User Stories in Gherkin format based on technical context.
    """

    def __init__(self, model=MODEL_NAME):
        self.model = model

    def generate_backlog(self, context, user_ideas):
        start_time = time.perf_counter()
        system_prompt = (
            "You are a Senior Product Owner. Requirements must be technical and Gherkin-compliant. "
            "Use RICE scoring for prioritization. Perform math calculations precisely."
        )

        # Dedent ensures the AI receives the table format without leading whitespace
        user_prompt = textwrap.dedent(
            f"""
            PROJECT CONTEXT: {context}
            USER IDEAS: {user_ideas}

            TASK:
            Create a PRIORITIZED BACKLOG in a Markdown Table.
            Formula: RICE = (Reach * Impact * Confidence) / Effort

            STRICT RULES:
            1. Sort the table by Score DESCENDING.
            2. For each task, provide a separate section with a User Story in GHERKIN format
            (Given, When, Then).
            3. Use only numeric values for RICE components (R, I, C, E).

            | Priority | Task Name | R | I | C | E | FINAL SCORE |
            |----------|-----------|---|---|---|---|-------------|
        """
        ).strip()

        response = ollama.chat(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )

        end_time = time.perf_counter()
        print(f"[Performance] Architect Agent processing time: {end_time - start_time:.2f}s")
        return response["message"]["content"]


class BacklogOrchestrator:
    def __init__(self, input_file_path):
        self.input_file = input_file_path
        self.reader = ProjectReaderAgent()
        self.architect = ProductOwnerAgent()

    def run(self):
        total_start = time.perf_counter()
        if not os.path.exists(self.input_file):
            return False

        with open(self.input_file, "r", encoding="utf-8") as f:
            user_ideas = f.read()

        context = self.reader.get_context()
        report = self.architect.generate_backlog(context, user_ideas)

        self._save(report)
        self._update_history_log(report)

        total_end = time.perf_counter()
        print(f"\n[Performance] Total Orchestration Time: {total_end - total_start:.2f}s")
        return True

    def _save(self, content):
        target_dir = os.path.join(PROJECT_ROOT, "docs", "backlog_output")
        os.makedirs(target_dir, exist_ok=True)
        filename = os.path.join(target_dir, f"{datetime.now().strftime('%Y%m%d_%H%M')}_backlog.md")
        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"[+] Prioritized backlog generated: {filename}")

    def _update_history_log(self, raw_markdown):
        history_path = os.path.join(PROJECT_ROOT, "docs", "project_history.csv")
        file_exists = os.path.isfile(history_path)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        logged_features = set()
        rows_to_log = []

        for line in raw_markdown.split("\n"):
            # Detecting table row: must have pipe and digits
            if "|" in line and any(char.isdigit() for char in line):
                parts = [p.strip() for p in line.split("|") if p.strip()]

                # We expect at least 7 parts based on our prompt table structure:
                # [Priority, Task Name, R, I, C, E, FINAL SCORE]
                if len(parts) >= 6:
                    feature_name = parts[1]  # Task Name is at index 1
                    if feature_name not in logged_features:
                        try:
                            # Numerical values are at indices 2, 3, 4, 5
                            r = float(parts[2])
                            i = float(parts[3])
                            c = float(parts[4])
                            e = float(parts[5])

                            score = round((r * i * c) / e, 2)
                            rows_to_log.append([timestamp, feature_name, r, i, c, e, score])
                            logged_features.add(feature_name)
                        except (ValueError, IndexError):
                            continue

        if rows_to_log:
            with open(history_path, mode="a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(
                        [
                            "Timestamp",
                            "Feature",
                            "Reach",
                            "Impact",
                            "Confidence",
                            "Effort",
                            "Score",
                        ]
                    )
                writer.writerows(rows_to_log)
            print(f"[+] Project history updated: {history_path}")


if __name__ == "__main__":
    ideas_path = os.path.join(PROJECT_ROOT, "backlog_ideas.txt")
    orchestrator = BacklogOrchestrator(ideas_path)
    orchestrator.run()
