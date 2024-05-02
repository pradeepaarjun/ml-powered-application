import great_expectations as ge


def generate_expectations(dataset_path, expectation_suite_path):
    dataset = ge.read_csv(dataset_path)
    expectation_suite = dataset.expect_table('profiling')
    expectation_suite.save(expectation_suite_path)

if __name__ == "__main__":
    dataset_path = 'path/to/your/dataset.csv'
    expectation_suite_path = 'path/to/expectation/suite.json'
    generate_expectations(dataset_path, expectation_suite_path)