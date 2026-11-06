"""Export current sources/evidence without secrets or runtime databases.

Packaging does not rewrite phase summaries, gates, version claims or activity logs.
Output must be outside this repository so repeated exports cannot include themselves.
"""
import argparse
import hashlib
import json
import shutil
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def allowed(path):
    relative = path.relative_to(ROOT)
    if any(part in {'.git', '__pycache__', '.venv', 'runtime', 'node_modules', 'tmp', 'credentials'} for part in relative.parts):
        return False
    name = path.name.lower()
    if (name.startswith('.env') or name.endswith('.env')) and name != '.env.example':
        return False
    if relative.as_posix().startswith(('artifacts/generated/', 'artifacts/phase5/backups/')):
        return False
    if tuple(suffix.lower() for suffix in path.suffixes[-2:]) in {('.tar', '.gz'), ('.sql', '.gz')}:
        return False
    return path.suffix.lower() not in {'.exe', '.winmd', '.pyc', '.sqlite', '.sqlite3', '.db', '.log', '.dump', '.tar', '.tgz', '.backup'}


def external(path):
    resolved = path.resolve()
    if resolved == ROOT or resolved.is_relative_to(ROOT) or ROOT.is_relative_to(resolved):
        raise ValueError('Choose a destination outside the repository and not its parent.')
    return resolved


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--mirror', type=Path)
    args = parser.parse_args()
    output = external(args.output_dir)
    mirror = external(args.mirror) if args.mirror else None
    if mirror and (output == mirror or output.is_relative_to(mirror) or mirror.is_relative_to(output)):
        raise ValueError('Output and mirror must be separate directories.')
    output.mkdir(parents=True, exist_ok=True)
    paths = sorted((p for p in ROOT.rglob('*') if p.is_file() and allowed(p)), key=lambda p: p.as_posix())
    manifest = []
    for path in paths:
        relative = path.relative_to(ROOT)
        if mirror:
            target = mirror / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
        manifest.append({'path': relative.as_posix(), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
        if relative.parts[0] == 'docs':
            target = output / 'project-docs' / path.relative_to(ROOT / 'docs')
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
    report = {'primary_repository': str(ROOT), 'files': manifest,
              'scope': 'Sources and recorded evidence; excludes secrets/generated data/runtime volumes.'}
    manifest_path = output / 'delivery_manifest.json'
    manifest_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding='utf-8')
    archive_path = output / 'BTL_DW_DELIVERY.zip'
    with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as archive:
        for path in paths:
            archive.write(path, path.relative_to(ROOT).as_posix())
        archive.write(manifest_path, 'delivery_manifest.json')
    print(json.dumps({'files': len(paths), 'archive': str(archive_path), 'docs': str(output / 'project-docs')}))


if __name__ == '__main__':
    main()
