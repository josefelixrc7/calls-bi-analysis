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

a.AnalysisBajaCalifornia()
s.CreateSegment(15000, '61047')

a.AnalysisJalisco()
s.CreateSegment(15000, '61048')

# Export Analysis
import functions.analysis as a

analysis = a.Analysis()
analysis.Export()