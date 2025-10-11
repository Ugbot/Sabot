#!/usr/bin/env python3
"""Debug ANTLR parse tree structure."""

from antlr4 import InputStream, CommonTokenStream, ParseTreeWalker
from antlr4_cypher import CypherLexer, CypherParser

def print_tree(ctx, indent=0):
    """Print parse tree structure."""
    ctx_type = type(ctx).__name__
    text = ctx.getText()[:50] if hasattr(ctx, 'getText') else ''

    print('  ' * indent + f'{ctx_type}: {text}')

    # Print available methods
    methods = [m for m in dir(ctx) if not m.startswith('_') and callable(getattr(ctx, m))]
    if indent < 3:  # Only for first few levels
        relevant_methods = [m for m in methods if m[0].islower() and not m.startswith('accept')]
        if relevant_methods:
            print('  ' * indent + f'  Methods: {relevant_methods[:10]}')

    # Recurse on children
    if hasattr(ctx, 'children') and ctx.children:
        for child in ctx.children[:5]:  # Limit to first 5 children
            if hasattr(child, '__class__') and 'Context' in child.__class__.__name__:
                print_tree(child, indent + 1)


# Parse simple query
query = 'MATCH (a)-[r]->(b) RETURN a.name'
print(f'Query: {query}\n')

input_stream = InputStream(query)
lexer = CypherLexer(input_stream)
tokens = CommonTokenStream(lexer)
parser = CypherParser(tokens)
tree = parser.query()

print_tree(tree)
