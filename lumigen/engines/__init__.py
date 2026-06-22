try:
    from sqlglot import Parser
    setattr(Parser, "_warn_unsupported", lambda self: self)
except:
    pass