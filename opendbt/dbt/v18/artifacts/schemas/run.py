import json
from pathlib import Path

from dbt.artifacts.schemas import run
from filelock import FileLock, Timeout

from opendbt.runtime_patcher import PatchClass
from opendbt.utils import Utils


# pylint: disable=too-many-ancestors
@PatchClass(module_name="dbt.artifacts.schemas.run", target_name="RunResultsArtifact")
@PatchClass(module_name="dbt.artifacts.schemas.run.v5.run", target_name="RunResultsArtifact")
class OpenDbtRunResultsArtifact(run.RunResultsArtifact):

    def run_info(self) -> dict:
        run_info_data = self.to_dict(omit_none=False)
        nodes = {}
        for r in self.results:
            key = r.unique_id
            execute_entry = next((item for item in r.timing if item.name == "execute"), None)
            run_completed_at = execute_entry.completed_at if execute_entry else None
            nodes[key] = {}
            nodes[key]['run_status'] = r.status
            nodes[key]['run_completed_at'] = run_completed_at.strftime("%Y-%m-%d %H:%M:%S")
            nodes[key]['run_message'] = r.message
            nodes[key]['run_failures'] = r.failures
            nodes[key]['run_adapter_response'] = r.adapter_response

        run_info_data['nodes'] = nodes
        run_info_data.pop('results', None)
        return run_info_data

    def write_run_info(self, path: str):
        run_info_file = Path(path).parent.joinpath("run_info.json")
        command = self.args.get('which', "NONE")
        if command not in ['run', 'build', 'test']:
            return

        lock_file = run_info_file.with_suffix(".json.lock")  # Use a distinct lock file extension
        data = {}
        try:
            # 2. Acquire lock (wait up to 10 seconds)
            lock = FileLock(lock_file, timeout=10)
            with lock:
                if run_info_file.exists() and run_info_file.stat().st_size > 0:
                    try:
                        with open(run_info_file, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        # Ensure it's a dictionary before merging
                        if not isinstance(data, dict):
                            print(f"Warning: Content of {run_info_file} is not a JSON object. Overwriting.")
                            data = {}
                    except json.JSONDecodeError:
                        print(f"Warning: Could not decode JSON from {run_info_file}. Overwriting.")
                    except Exception as e:
                        print(f"Error reading {run_info_file}: {e}. Starting fresh.")

                new_data = self.run_info()
                data = Utils.merge_dicts(data, new_data)

                try:
                    with open(run_info_file, 'w', encoding='utf-8') as f:
                        json.dump(data, f)
                except Exception as e:
                    print(f"Error writing merged data to {run_info_file}: {e}")

        except Timeout:
            print(
                f"Error: Could not acquire lock on {lock_file} within 10 seconds. Skipping update for {run_info_file}.")
        except Exception as e:
            # Catch other potential errors during locking or file operations
            print(f"An unexpected error occurred processing {run_info_file}: {e}")

    def write(self, path: str):
        super().write(path)
        self.write_run_info(path=path)
