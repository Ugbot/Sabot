#!/usr/bin/env python3
"""
Fix all cdef declarations inside try/if/while blocks in libpq_conn.pyx.
Move them to the top of their function.
"""

import re

file_path = "/Users/bengamble/Sabot/sabot/_cython/connectors/postgresql/libpq_conn.pyx"

with open(file_path, 'r') as f:
    lines = f.readlines()

# Find all functions and their cdef declarations
fixed_lines = []
i = 0
while i < len(lines):
    line = lines[i]

    # Check if we're starting a function
    if line.strip().startswith(('def ', 'cdef ', 'cpdef ', 'async def')):
        # Collect function definition (may span multiple lines)
        func_lines = [line]
        i += 1

        # Collect docstring and find function body start
        in_docstring = '"""' in line or "'''" in line
        docstring_lines = []
        body_start_idx = None

        while i < len(lines):
            curr_line = lines[i]
            func_lines.append(curr_line)

            if in_docstring:
                if '"""' in curr_line or "'''" in curr_line:
                    if curr_line.count('"""') == 2 or curr_line.count("'''") == 2:
                        in_docstring = False
                    elif curr_line.strip().endswith('"""') or curr_line.strip().endswith("'''"):
                        in_docstring = False
                docstring_lines.append(curr_line)
                i += 1
                continue

            # Look for first non-cdef statement (body starts)
            stripped = curr_line.strip()
            if stripped and not stripped.startswith('cdef ') and not stripped.startswith('#'):
                if not stripped.startswith('"""') and not stripped.startswith("'''"):
                    body_start_idx = i
                    break

            i += 1

        if body_start_idx:
            # Collect cdef declarations from body
            cdef_decls = []
            body_lines = []

            j = body_start_idx
            while j < len(lines):
                body_line = lines[j]

                # Check if we've reached next function
                if body_line.strip().startswith(('def ', 'cdef ', 'cpdef ', 'async def')) and body_line[0] not in (' ', '\t'):
                    break

                # Find cdef declarations inside blocks (indented more than function level)
                if '        cdef ' in body_line and '=' in body_line:
                    # Extract variable name and type
                    match = re.search(r'cdef\s+(.+?)\s+(\w+)\s*=', body_line)
                    if match:
                        var_type = match.group(1)
                        var_name = match.group(2)
                        # Move to top-level cdef
                        cdef_decls.append(f"        cdef {var_type} {var_name}\n")
                        # Replace with assignment only
                        indent = len(body_line) - len(body_line.lstrip())
                        body_line = ' ' * indent + var_name + body_line.split('=', 1)[1]

                body_lines.append(body_line)
                j += 1

            # Write function with cdef declarations at top
            fixed_lines.extend(func_lines[:body_start_idx - i + len(func_lines)])
            fixed_lines.extend(cdef_decls)
            fixed_lines.extend(body_lines)
            i = j
        else:
            fixed_lines.extend(func_lines)
    else:
        fixed_lines.append(line)
        i += 1

# Write fixed file
with open(file_path, 'w') as f:
    f.writelines(fixed_lines)

print(f"✅ Fixed cdef declarations in {file_path}")
