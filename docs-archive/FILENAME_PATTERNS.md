# Context-Specific Filename Patterns

## Overview

Each context in `contexts.yaml` can have its own `filename_pattern` that overrides the global pattern from `config.yaml`. This allows different document categories to have different filename structures optimized for their use case.

## Configuration

### Global Pattern (config.yaml)

The global pattern in `config.yaml` serves as the default:

```yaml
# Pattern for constructing target filename
# Available placeholders: {type}, {date}, {sender}, {topic}, {subject}
filename_pattern: "{type}-{date}-{sender}-{topic}-{subject}"
```

### Context-Specific Patterns (contexts.yaml)

Each context can override the global pattern:

```yaml
- name: work
  description: Work-related documents
  filename_pattern: "{type}-{date}-{sender}-{topic}"  # Override: shorter, no subject
  instructions:
    # ... instructions ...

- name: private
  description: Personal documents
  filename_pattern: "{context}-{type}-{date}-{sender}"  # Override: include context
  instructions:
    # ... instructions ...

- name: taxes
  description: Tax documents
  # No filename_pattern specified - uses global pattern from config.yaml
  instructions:
    # ... instructions ...
```

## Available Placeholders

All placeholders from metadata extraction:

| Placeholder | Description | Example |
|-------------|-------------|---------|
| `{context}` | Determined context name | `work`, `private`, `taxes` |
| `{type}` | Document type | `invoice`, `contract`, `receipt` |
| `{date}` | Document date (YYYY-MM-DD) | `2024-01-15` |
| `{sender}` | Document sender/issuer | `acme_corp`, `utility_company` |
| `{topic}` | Topic/dossier | `project_alpha`, `tax_year_2024` |
| `{subject}` | Specific subject | `q4_payment`, `annual_report` |

**Note:** The `{context}` placeholder is only available when using contexts. It contains the name of the determined context.

## Pattern Selection Logic

1. Document is processed through mrdocument
2. If contexts are used, context is determined (e.g., "work")
3. Watcher looks for `filename_pattern` in the determined context
4. If found, uses context-specific pattern
5. If not found, uses global pattern from `config.yaml`
6. Filename is generated using the selected pattern

## Use Cases and Examples

### Use Case 1: Include Context in Filename

**Scenario:** You want to easily see which context a document belongs to.

```yaml
- name: work
  description: Work documents
  filename_pattern: "{context}-{type}-{date}-{sender}"
  
- name: private
  description: Personal documents  
  filename_pattern: "{context}-{type}-{date}-{sender}"
```

**Result:**
- Work invoice → `work-invoice-2024-01-15-acme_corp.pdf`
- Private bill → `private-bill-2024-01-15-utility_company.pdf`

### Use Case 2: Different Priorities per Context

**Scenario:** Different contexts need different field priorities.

```yaml
- name: freelance
  description: Freelance work
  filename_pattern: "{topic}-{type}-{date}"  # Client first
  
- name: taxes
  description: Tax documents
  filename_pattern: "{topic}-{type}-{date}-{sender}"  # Tax year first
  
- name: household
  description: Household bills
  filename_pattern: "{topic}-{date}-{sender}"  # Category, date, provider
```

**Result:**
- Freelance invoice → `client_x-invoice-2024-01-15.pdf`
- Tax receipt → `tax_year_2024-receipt-2024-01-15-vendor.pdf`
- Household bill → `electricity-2024-01-15-power_company.pdf`

### Use Case 3: Shorter Filenames for Some Contexts

**Scenario:** Some contexts don't need all fields.

```yaml
- name: screenshots
  description: Screenshots and images
  filename_pattern: "{context}-{date}"  # Just context and date
  
- name: receipts
  description: Purchase receipts
  filename_pattern: "{type}-{date}-{sender}"  # No topic needed
```

**Result:**
- Screenshot → `screenshots-2024-01-15.pdf`
- Receipt → `receipt-2024-01-15-store_name.pdf`

### Use Case 4: Mix of Custom and Global Patterns

**Scenario:** Only some contexts need custom patterns.

```yaml
# config.yaml
filename_pattern: "{type}-{date}-{sender}-{topic}-{subject}"

# contexts.yaml
- name: legal
  description: Legal documents
  filename_pattern: "{type}-{topic}-{date}-{sender}"  # Custom: matter reference first
  
- name: work
  description: Work documents
  # No custom pattern - uses global pattern
  
- name: private
  description: Personal documents
  # No custom pattern - uses global pattern
```

**Result:**
- Legal document → `contract-matter_2024_123-2024-01-15-law_firm.pdf`
- Work invoice → `invoice-2024-01-15-acme_corp-project_alpha-q4_payment.pdf` (global)
- Private bill → `bill-2024-01-15-utility_company-electricity-monthly_statement.pdf` (global)

## Pattern Design Best Practices

### 1. Consistent Field Order

Keep similar fields in the same order across contexts for easier navigation:

**Good:**
```yaml
- name: work
  filename_pattern: "{type}-{date}-{sender}-{topic}"
  
- name: freelance  
  filename_pattern: "{type}-{date}-{topic}-{sender}"  # Similar order
```

**Less Good:**
```yaml
- name: work
  filename_pattern: "{type}-{date}-{sender}-{topic}"
  
- name: freelance
  filename_pattern: "{sender}-{topic}-{type}-{date}"  # Completely different order
```

### 2. Most Important Field First

Put the most distinguishing/important field first:

```yaml
- name: taxes
  filename_pattern: "{topic}-{type}-{date}"  # Tax year first - most important
  
- name: freelance
  filename_pattern: "{topic}-{type}-{date}"  # Client first - most important
  
- name: household
  filename_pattern: "{topic}-{date}-{sender}"  # Category first - most important
```

### 3. Include Date for Chronology

Always include `{date}` for chronological sorting:

```yaml
filename_pattern: "{type}-{date}-{sender}"  # ✓ Has date
filename_pattern: "{type}-{sender}"          # ✗ No date
```

### 4. Avoid Too Many Fields

Keep patterns concise - usually 3-4 fields is enough:

```yaml
filename_pattern: "{type}-{date}-{sender}-{topic}"              # ✓ 4 fields, concise
filename_pattern: "{context}-{type}-{date}-{sender}-{topic}-{subject}"  # ✗ 6 fields, too long
```

### 5. Use {context} Strategically

Include `{context}` when:
- You want clear visual separation between contexts
- Filenames might otherwise be ambiguous
- You sort/search files by context

Don't include `{context}` when:
- Context is obvious from other fields
- You want shorter filenames
- Files are already organized in context-specific folders

## Testing Patterns

1. **Start simple:** Test with 2-3 contexts and basic patterns
2. **Process samples:** Process a few test documents from each context
3. **Check results:** Verify filenames make sense and are distinguishable
4. **Iterate:** Adjust patterns based on actual results
5. **Document:** Add comments in `contexts.yaml` explaining pattern choices

## Filename Collisions

If the generated filename already exists, a UUID suffix is automatically added:

```
Original: invoice-2024-01-15-acme_corp.pdf
Collision: invoice-2024-01-15-acme_corp-a3b4c5d6.pdf
```

This happens regardless of pattern used and ensures no files are overwritten.

## Debugging

Enable debug logging to see pattern selection:

```bash
LOG_LEVEL=DEBUG docker compose logs -f syncthing
```

Look for:
```
[alice] Using pattern for context 'work': {type}-{date}-{sender}-{topic}
[alice] Generated filename: invoice-2024-01-15-acme_corp-project_alpha.pdf
```

## Migration

### From Global Pattern Only

If you're currently using only the global pattern in `config.yaml`:

1. **No changes needed** - existing behavior continues
2. **Add patterns gradually** - add `filename_pattern` to one context at a time
3. **Test each change** - process a document and verify the result

### Changing an Existing Pattern

When changing a context's pattern:

1. Old files keep their names (not renamed)
2. New files use new pattern
3. You'll have mixed patterns in your archive - this is fine
4. Consider documenting the change date if needed

## Examples Repository

See `contexts.yaml.example` for complete examples with patterns for:
- Freelance work
- Household bills
- Tax documents
- Legal documents
- Medical records

Each example includes pattern choice rationale.
