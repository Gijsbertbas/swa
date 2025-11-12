import subprocess


class GoogleExtractor:

    def rclone_sync(self, source: str, dest: str, filter_filename: str):
        try:
            result = subprocess.run(
                [
                    'rclone', 'sync',
                    source,
                    dest,
                    '--filter-from', filter_filename,
                    '--progress', 
                    # '--verbose'
                ],
                check=True,
                # capture_output=True,
                text=True
            )
            
            print("\nRclone output:")
            print(result.stdout)
            print("\nRclone sync completed successfully!")
            
        except subprocess.CalledProcessError as e:
            print(f"\nRclone sync failed with error code {e.returncode}")
            print(f"Error output:\n{e.stderr}")
        except FileNotFoundError:
            print("\nError: rclone command not found. Make sure rclone is installed and in your PATH.")
