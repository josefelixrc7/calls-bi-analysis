# Clean segments
import functions.segments as s

s.CleanSegments()

# Clean DBs used
import functions.segments as s

s.CleanDatabasesUsed()

# Use DB
import functions.segments as s

s.UseDatabaseType('WIN2MIN')

# Exclusions
import functions.exclusions as e

e.ExcludeBacklist()
e.ExcludeSales()
e.ExcludeOverused()
e.ExcludeNoreusable()
e.ExcludeNoDuration()

# Analysis
import functions.analysis
import functions.segments as s

a = functions.analysis.Analysis()

a.AnalysisMinutos1Plus()
s.CreateSegment(7000, '61050')

# Export Analysis
import functions.analysis as a

analysis = a.Analysis()
analysis.Export()