def main():
    import argparse
    from my_package.main import run

    parser = argparse.ArgumentParser(description='My Python Project CLI')
    parser.add_argument('--option', type=str, help='An option for the application')

    args = parser.parse_args()

    if args.option:
        run(args.option)
    else:
        run()