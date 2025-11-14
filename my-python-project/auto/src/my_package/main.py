from typing import Optional
from my_package.server import run as run_server

def run(option: Optional[str] = None):
    if option is None or option == "serve":
        print("Starting server on http://0.0.0.0:8000")
        run_server()
    elif option == "import":
        print("Import option selected - no import implemented")
    else:
        print(f"Unknown option: {option}")