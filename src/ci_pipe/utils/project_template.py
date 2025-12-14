import os


def _read_template(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def load_project_templates():
    module_directory = os.path.dirname(os.path.abspath(__file__))
    templates_directory = os.path.join(module_directory, "..", "project_templates")

    prj_template = _read_template(
        os.path.join(templates_directory, "prj_template.json")
    )
    plane_template = _read_template(
        os.path.join(templates_directory, "single_plane_template.json")
    )
    return plane_template, prj_template