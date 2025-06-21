import typer
from .broker import SimpleMessageBroker
import asyncio

def main(host: str = typer.Option("localhost", help="Host to bind the server to."),
         port: int = typer.Option(8888, help="Port to listen on.")):
    """Run the SimpleMessageBroker server."""
    broker = SimpleMessageBroker(host=host, port=port)
    asyncio.run(broker.start())

if __name__ == "__main__":
    typer.run(main)
