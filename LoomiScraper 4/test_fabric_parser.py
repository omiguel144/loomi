from fabric_parser import parse_fabric_breakdown

examples = [
    "100% Organic Cotton – Your new favorite hoodie",
    "80% cotton, 20% linen",
    "Wildflower/Marble: 80% cotton, 20% linenSolids: 100% cottonEasily worn...",
    "40% OFF EVERY HAREM PANT",  # should be ignored
]

for text in examples:
    breakdown, tags = parse_fabric_breakdown(text)
    print("RAW:", text)
    print(" -> breakdown:", breakdown)
    print(" -> tags:", tags)
    print("---")
