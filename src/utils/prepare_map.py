import json
import os


def prepare_topojson_keys(input_path, output_path):
    """
    Reads a TopoJSON file, aligns its property names with the project's 'region' column.

    Target 'region' values (must match Spark output exactly):
    "Dolnośląskie", "Kujawsko-pomorskie", "Lubelskie", "Lubuskie", "Łódzkie",
    "Małopolskie", "Mazowieckie", "Opolskie", "Podkarpackie", "Podlaskie",
    "Pomorskie", "Śląskie", "Świętokrzyskie", "Warmińsko-mazurskie",
    "Wielkopolskie", "Zachodniopomorskie"
    """

    if not os.path.exists(input_path):
        print(f"[!] Error: Input file not found at {input_path}")
        return

    print(f"--- Processing TopoJSON Map: {os.path.basename(input_path)} ---")

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # UPDATED MAPPING:
    # Keys match the simplified names found in 'poland_voivodeships.json'
    # Values match the official Polish names from Spark/GUS
    name_map = {
        "dolnoslaskie": "Dolnośląskie",
        "kujawsko-pomorskie": "Kujawsko-pomorskie",
        "lubelskie": "Lubelskie",
        "lubuskie": "Lubuskie",
        "lódzkie": "Łódzkie",
        "malopolskie": "Małopolskie",
        "mazowieckie": "Mazowieckie",
        "opolskie": "Opolskie",
        "podkarpackie": "Podkarpackie",
        "podlaskie": "Podlaskie",
        "pomorskie": "Pomorskie",
        "slaskie": "Śląskie",
        "swietokrzyskie": "Świętokrzyskie",
        "warminsko-mazurskie": "Warmińsko-mazurskie",
        "wielkopolskie": "Wielkopolskie",
        "zachodniopomorskie": "Zachodniopomorskie",
    }

    if "objects" not in data:
        print("[!] Error: This does not look like a TopoJSON file (missing 'objects' key).")
        return

    updated_count = 0

    for obj_name in data["objects"]:
        geometries = data["objects"][obj_name].get("geometries", [])

        for geo in geometries:
            props = geo.get("properties", {})

            # The uploaded file uses 'name' and 'NAME_1'
            # We use 'name' as it contains the Polish-like strings (e.g., 'Slaskie')
            source_name = props.get("name") or props.get("nazwa")

            if source_name:
                clean_source = str(source_name).lower().strip()
                if clean_source in name_map:
                    # Power BI Shape Map binds to the 'name' property by default
                    props["name"] = name_map[clean_source]
                    updated_count += 1
                else:
                    print(f"      [Warning] No mapping found for: {source_name}")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print(f"[OK] Processed {updated_count} regions inside TopoJSON.")
    print(f"[OK] Saved to: {output_path}")
    print("\nNext step: Load this file directly into Power BI (Shape Map -> Custom Map).")


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))

    input_file = os.path.join(project_root, "assets", "maps", "poland_voivodeships.json")
    output_file = os.path.join(project_root, "assets", "maps", "poland_regions_fixed.json")

    prepare_topojson_keys(input_file, output_file)
