import json
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from anytree import Node, RenderTree


class Plotter:
    def __init__(self, console=None):
        self.console = console or Console()

    def get_step_info(self, trace, step_number, branch, show_parameters=True):
        step = self._get_step(trace, step_number, branch)
        if not step:
            return

        table = self._build_table(step, show_parameters)
        self.console.print(table)

    def _get_step(self, trace, step_number, branch):
        try:
            steps = trace[branch]["steps"]
        except KeyError:
            self.console.print(f"[bold red]Branch '{branch}' not found in trace[/bold red]")
            return None

        for step in steps:
            if step["index"] == step_number:
                return step

        self.console.print(f"[bold red]Step {step_number} not found in branch '{branch}'[/bold red]")
        return None

    def _build_table(self, step, show_parameters):
        table = Table(title=f"Step {step['index']} - {step['name']}", show_lines=True)
        table.add_column("Field", style="cyan", no_wrap=True)
        table.add_column("Value", style="yellow")

        params = step.get("params", {})
        outputs = step.get("outputs", {})

        table.add_row("Name", step.get("name", ""))
        if show_parameters and params:
            params_str = "\n".join(f"{k}: {v}" for k, v in params.items())
            table.add_row("Parameters", params_str)

        outputs_str = json.dumps(outputs, indent=2)
        table.add_row("Outputs", outputs_str)

        return table

    def get_all_trace_from_branch(self, trace, branch):
        branch_trace = trace.get(branch)
        if not branch_trace:
            self.console.print(f"[bold red]Branch '{branch}' not found[/bold red]")
            return

        steps = branch_trace.get("steps", [])
        if not steps:
            self.console.print(f"[yellow]No steps found in branch '{branch}'[/yellow]")
            return

        panels = self._build_trace_panels(steps)

        self.console.print(f"\n[bold underline]Pipeline Trace of branch: {branch}[/bold underline]\n")
        self.console.print(*panels, justify="center")

    def _build_trace_panels(self, steps):
        panels = [
            Panel(
                f"[bold]{step['index']}.[/bold] {step['name']}\n"
                f"Params: {', '.join(step['params'].keys()) or 'None'}",
                padding=(1, 2),
            )
            for step in steps
        ]

        items = []
        for i, p in enumerate(panels):
            items.append(p)
            if i < len(panels) - 1:
                items.append("⬇")
        return items