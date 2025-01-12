# Clean segments
import functions.segments as s

s.CleanSegments()

# Clean DBs used
s.CleanDatabasesUsed()

# Use DB
s.UseDatabaseType('Referidos')

# Exclusions
import functions.exclusions as e

e.ExcludeBacklist()
e.ExcludeSales()
e.ExcludeOverused()
e.ExcludeNoDuration()

# Analysis
import functions.analysis
import functions.segments as s

a = functions.analysis.Analysis()

s.SegmentLeft()
s.CreateSegment(7000, '61049')

# Export Analysis
import functions.analysis as a

analysis = a.Analysis()
analysis.Export()