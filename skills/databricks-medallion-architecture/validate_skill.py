import re
import os
from pathlib import Path

skill_file = 'SKILL.md'
with open(skill_file, 'r', encoding='utf-8') as f:
    content = f.read()

print('=' * 80)
print('QUALITY VALIDATION REPORT')
print('=' * 80)
print()

# Track errors
errors = []
warnings = []

# 1. Check for code fences without language specification
print('1. CHECKING CODE FENCES FOR LANGUAGE SPECIFICATION...')
lines = content.split('\n')
code_fence_count = 0
for i, line in enumerate(lines, 1):
    if line.startswith('`'):
        code_fence_count += 1
        lang = line[3:].strip()
        if not lang:
            errors.append(f'Line {i}: Code fence missing language specification')
            print(f'   ERROR: Line {i}: Code fence missing language specification')
        else:
            print(f'   OK: Line {i}: Code fence has language: {lang}')

print(f'   Total code fences checked: {code_fence_count}')

print()
print('2. CHECKING MERMAID DIAGRAMS FOR VALID SYNTAX...')
# Find all mermaid blocks
mermaid_pattern = r'`mermaid\n(.*?)\n`'
mermaid_blocks = re.findall(mermaid_pattern, content, re.DOTALL)
print(f'   Found {len(mermaid_blocks)} Mermaid diagram(s)')
for idx, block in enumerate(mermaid_blocks, 1):
    lines = block.strip().split('\n')
    first_line = lines[0].strip()
    print(f'   Diagram {idx}: Type: {first_line}')
    # Basic syntax check - look for matching braces/brackets
    if block.count('{') != block.count('}'):
        errors.append(f'Mermaid diagram {idx}: Mismatched braces')
        print(f'      ERROR: Mismatched braces')
    else:
        print(f'      OK: Braces match')

print()
print('3. CHECKING MARKDOWN FORMATTING...')
# Check for common markdown issues
# Check for proper heading hierarchy
heading_pattern = r'^(#+)\s+'
headings = re.findall(heading_pattern, content, re.MULTILINE)
heading_levels = [len(h) for h in headings]
print(f'   Found {len(heading_levels)} heading(s)')
print(f'   Heading levels: {heading_levels}')

# Check for proper spacing (ensure blank lines before headings)
lines = content.split('\n')
for i in range(1, len(lines)):
    if re.match(r'^#{1,6}\s+', lines[i]):
        if lines[i-1].strip() != '' and not re.match(r'^#{1,6}\s+', lines[i-1]):
            warnings.append(f'Line {i+1}: Heading should have blank line before it')
            print(f'   WARNING: Line {i+1}: No blank line before heading')

print()
print('4. CHECKING FOR VALID RELATIVE PATHS IN CROSS-REFERENCES...')
# Check for image references and links
image_pattern = r'!\[([^\]]*)\]\(([^\)]+)\)'
link_pattern = r'(?<!!)\[([^\]]+)\]\(([^\)]+)\)'

# Get current directory for path checking
base_path = Path('.')

# Check images
images = re.finditer(image_pattern, content)
image_count = 0
for match in images:
    image_count += 1
    alt_text, image_path = match.groups()
    # Only check relative paths (not URLs)
    if not (image_path.startswith('http') or image_path.startswith('#')):
        full_path = base_path / image_path
        if not full_path.exists():
            errors.append(f'Image reference broken: {image_path}')
            print(f'   ERROR: Image not found: {image_path}')
        else:
            print(f'   OK: Image found: {image_path}')
    else:
        print(f'   OK: External/anchor reference: {image_path}')

print(f'   Total images checked: {image_count}')

# Check links
links = re.finditer(link_pattern, content)
link_count = 0
for match in links:
    link_count += 1
    link_text, link_path = match.groups()
    # Only check relative paths (not URLs)
    if not (link_path.startswith('http') or link_path.startswith('#') or link_path.startswith('mailto:')):
        full_path = base_path / link_path
        if not full_path.exists():
            errors.append(f'Link reference broken: {link_path}')
            print(f'   ERROR: Link target not found: {link_path}')
        else:
            print(f'   OK: Link target found: {link_path}')
    else:
        print(f'   OK: External/anchor reference: {link_path}')

print(f'   Total links checked: {link_count}')

print()
print('=' * 80)
print('VALIDATION SUMMARY')
print('=' * 80)
print(f'Total Errors Found: {len(errors)}')
print(f'Total Warnings Found: {len(warnings)}')
if errors:
    print()
    print('ERRORS:')
    for error in errors:
        print(f'  - {error}')
if warnings:
    print()
    print('WARNINGS:')
    for warning in warnings:
        print(f'  - {warning}')

if not errors and not warnings:
    print('✓ All validation checks passed!')
