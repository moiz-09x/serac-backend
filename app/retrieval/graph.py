from langgraph.graph import END, StateGraph

from app.retrieval.nodes import assemble, decompose, search, synthesise, verify
from app.retrieval.state import RetrievalState


def _route_after_search(state: RetrievalState) -> str:
    return "assemble" if state.get("context_items") else "no_context"


async def _no_context(state: RetrievalState) -> dict:
    return {"answer": "No relevant context found in the knowledge graph for this question."}


def build_graph():
    graph = StateGraph(RetrievalState)

    graph.add_node("decompose", decompose)
    graph.add_node("search", search)
    graph.add_node("assemble", assemble)
    graph.add_node("synthesise", synthesise)
    graph.add_node("verify", verify)
    graph.add_node("no_context", _no_context)

    graph.set_entry_point("decompose")
    graph.add_edge("decompose", "search")
    graph.add_conditional_edges("search", _route_after_search, {
        "assemble": "assemble",
        "no_context": "no_context",
    })
    graph.add_edge("assemble", "synthesise")
    graph.add_edge("synthesise", "verify")
    graph.add_edge("verify", END)
    graph.add_edge("no_context", END)

    return graph.compile()
