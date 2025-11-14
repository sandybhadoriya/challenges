import unittest
from src.my_package.main import run
from src.my_package.cli import main

class TestBasicFunctionality(unittest.TestCase):

    def test_run_function(self):
        # Add assertions to test the run function
        self.assertIsNone(run())  # Example assertion, modify as needed

    def test_cli_main_function(self):
        # Add assertions to test the CLI main function
        self.assertIsNone(main())  # Example assertion, modify as needed

if __name__ == '__main__':
    unittest.main()