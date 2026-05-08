"""
Verifica que no queda ninguna referencia a 'annealyn' (typo)
en nombres de archivo ni en el contenido de los archivos.
"""

from pathlib import Path

ROOT = Path(".")
TYPO = "annealyn"
EXTENSIONS = {".py", ".jsonl", ".json", ".csv", ".md", ".txt", ".cnf", ".toml"}

errors_names = []
errors_content = []

for path in ROOT.rglob("*"):
    # Ignorar directorios ocultos y entornos virtuales
    if any(part.startswith(".") or part in ("dwave-env", "__pycache__", "node_modules")
           for part in path.parts):
        continue

    # Comprobar nombre de archivo/carpeta
    if TYPO in path.name:
        errors_names.append(str(path))

    # Comprobar contenido solo de archivos de texto relevantes
    if path.is_file() and path.suffix in EXTENSIONS:
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
            lines = [i + 1 for i, line in enumerate(text.splitlines()) if TYPO in line]
            if lines:
                errors_content.append((str(path), lines))
        except Exception as e:
            print(f"  [WARN] No se pudo leer {path}: {e}")

# --- Resultados ---
print("=" * 60)
print(f"Buscando '{TYPO}' en: {ROOT.resolve()}")
print("=" * 60)

if errors_names:
    print(f"\n❌ ARCHIVOS/CARPETAS con el typo en el nombre ({len(errors_names)}):")
    for p in errors_names:
        print(f"   {p}")
else:
    print("\n✅ Ningún archivo o carpeta con el typo en el nombre.")

if errors_content:
    print(f"\n❌ ARCHIVOS con el typo en el contenido ({len(errors_content)}):")
    for path, lines in errors_content:
        print(f"   {path}  →  líneas {lines}")
else:
    print("✅ Ningún archivo con el typo en el contenido.")

print()
if not errors_names and not errors_content:
    print("✅ Todo limpio. El renombrado está completo.")
else:
    total = len(errors_names) + len(errors_content)
    print(f"⚠️  Se encontraron {total} problema(s) pendientes.")
print("=" * 60)
