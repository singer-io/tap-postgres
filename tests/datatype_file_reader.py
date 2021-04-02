import os

potential_paths = [
    'tests/',
    '../tests/'
    'tap-postgres/tests/',
    '../tap-postgres/tests/',
]

datatype_to_file = {
    "text": "text_datatype.txt",
}

def _go_to_tests_directory():
    for path in potential_paths:
        if os.path.exists(path):
            os.chdir(path)
            return os.getcwd()
    raise NotImplementedError("This reader cannot run from {}".format(os.getcwd()))


def read_in(datatype: str = "text"):
    print("Acquiring path to tests directory.")
    cwd = _go_to_tests_directory()

    filename = datatype_to_file[datatype]

    print("Reading contents of {}.".format(filename))
    with open(cwd + "/" + filename, "r") as data:
        contents = data.read()

    return contents
