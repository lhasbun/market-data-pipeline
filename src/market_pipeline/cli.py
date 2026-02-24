import typer
from .pipeline import run_pipeline
from .validate import validate_symbol
from .config_loader import load_config
from .load import load_existing

# Main CLI application
app = typer.Typer(help="Market Data Pipeline CLI")
# Sub CLI applications
validate_app = typer.Typer(help="Validation commands")
##ingest_app = typer.Typer(help="Ingestion commands")

@app.command()
def update(symbol: str | None = None):
    """
    Run full pipeline.
    If --symbol provided, only that symbol is processed.
    Otherwise, all symbols from config are procesed.

    :param symbol: Stock ticker symbol
    :type symbol: str
    """

    run_pipeline(symbol=symbol)

@validate_app.command("symbol")
def validate_symbol_cmd(symbol: str):
    """
    Validate stored data for a given symbol.

    :param symbol: Stock ticker symbol
    :type symbol: str
    """
    validate_symbol(symbol)
    typer.echo(f"Validation completed for {symbol}")

@validate_app.command("all")
def validate_all_cmd():
    """
    Validate stored data for all symbols stored in config.
    """

    config = load_config()
    symbols = config["symbols"] # Laod all symbols from config file

    for sym in symbols:
        df = load_existing(sym) # Check if existing dataset
        if df is None:
            typer.echo(f"No data found for {sym}, skipping")
            continue
        validate_symbol(sym) # Validate DataFrame
        typer.echo(f"Validation completed for {sym}")

def main():
    app()

# Register CLI subcommand
app.add_typer(validate_app, name="validate")

