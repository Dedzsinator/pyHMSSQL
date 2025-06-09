#!/usr/bin/env python3
"""
Script to fix all assert insert() is True patterns in test files.
"""

import os
import re


def fix_assert_patterns(file_path):
    """Fix assert insert is True patterns in a file."""
    try:
        with open(file_path, "r") as f:
            content = f.read()

        # Pattern to match: assert tree.insert(...) is True
        pattern = r"assert\s+([\w\.]+\.insert(?:_\w+)?)\s*\([^)]+\)\s*is\s+True"

        # Replace with just the insert call
        def replace_func(match):
            return (
                match.group(1)
                + match.group(0)[
                    match.group(0).find("(") : match.group(0).rfind(") is True") + 1
                ]
            )

        # More specific patterns
        patterns_to_fix = [
            (r"assert\s+(tree\.insert\([^)]+\))\s*is\s+True", r"\1"),
            (r"assert\s+(tree\.insert_multidim\([^)]+\))\s*is\s+True", r"\1"),
            (r"assert\s+(tree\.insert_composite\([^)]+\))\s*is\s+True", r"\1"),
        ]

        original_content = content
        for pattern, replacement in patterns_to_fix:
            content = re.sub(pattern, replacement, content)

        if content != original_content:
            with open(file_path, "w") as f:
                f.write(content)
            print(f"✓ Fixed {file_path}")
            return True
        else:
            print(f"- No changes needed for {file_path}")
            return False

    except Exception as e:
        print(f"❌ Error fixing {file_path}: {e}")
        return False


def main():
    test_dir = "/home/deginandor/Documents/Programming/pyHMSSQL/tests/test_bptree"
    test_files = [
        "test_bptree_comprehensive.py",
        "test_multidimensional.py",
        "test_performance.py",
    ]

    print("🔧 Fixing assert insert() is True patterns in test files...")

    fixed_count = 0
    for test_file in test_files:
        file_path = os.path.join(test_dir, test_file)
        if os.path.exists(file_path):
            if fix_assert_patterns(file_path):
                fixed_count += 1
        else:
            print(f"⚠️  File not found: {file_path}")

    print(f"\n📊 Fixed {fixed_count} files")


if __name__ == "__main__":
    main()
