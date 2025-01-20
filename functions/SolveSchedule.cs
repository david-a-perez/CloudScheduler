using Google.OrTools.Sat;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;

namespace functions
{
    public static class SolveSchedule
    {
        [Function(nameof(SolveSchedule))]
        public static async Task<ScheduleOutput> RunOrchestrator(
            [OrchestrationTrigger] TaskOrchestrationContext context,
            ScheduleInput input)
        {
            ILogger logger = context.CreateReplaySafeLogger(nameof(SolveSchedule));
            logger.LogInformation("Saying hello.");

            return await context.CallActivityAsync<ScheduleOutput>(nameof(Solve), input);
        }

        [Function(nameof(Solve))]
        public static ScheduleOutput Solve([ActivityTrigger] ScheduleInput input, FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("Solve");
            logger.LogInformation("Processing {input}", input);

            int people = input.People.Count;

            if (input.Days.Any(dayInput => dayInput.AvailabilityPerPerson.Count != people))
            {
                throw new ArgumentException("AvailabilityPerPerson has wrong person count");
            }

            if (input.Days.Any(dayInput => dayInput.DayPreferencePerPerson.Count != people))
            {
                throw new ArgumentException("DayPreferencePerPerson has wrong person count");
            }

            CpModel model = new();

            List<List<BoolVar>> matrix = input.Days.Select((inputDay, dayIdx) => inputDay.AvailabilityPerPerson.Select((_available, personIdx) => model.NewBoolVar($"{dayIdx}_{personIdx}")).ToList()).ToList();

            foreach ((List<BoolVar> dayModel, DayInput dayInput) in matrix.Zip(input.Days))
            {
                model.Add(new BoundedLinearExpression(dayInput.MinimumAssigned, LinearExpr.Sum(dayModel), dayInput.MaximumAssigned));
            }

            foreach ((List<BoolVar> dayModel, DayInput dayInput) in matrix.Zip(input.Days))
            {
                foreach ((BoolVar assigned, bool availaible) in dayModel.Zip(dayInput.AvailabilityPerPerson))
                {
                    if (!availaible)
                    {
                        model.Add(assigned.AsExpr() == 0);
                    }
                }
            }

            foreach ((List<BoolVar> personModel, PersonInput personInput) in Transpose(matrix).Zip(input.People))
            {
                model.Add(new BoundedLinearExpression(personInput.MinimumAssigned, LinearExpr.Sum(personModel), personInput.MaximumAssigned));
            }

            LinearExpr dayPreferences = LinearExpr.WeightedSum(matrix.SelectMany(dayModel => dayModel), input.Days.SelectMany(dayInput => dayInput.DayPreferencePerPerson));
            LinearExpr assignments = LinearExpr.Sum(matrix.SelectMany(dayModel => dayModel)) * input.AssignmentWeight;


            model.Maximize(dayPreferences + assignments);

            CpSolver solver = new()
            {
                // Adds a time limit. Parameters are stored as strings in the solver.
                StringParameters = "max_time_in_seconds:10.0"
            };

            CpSolverStatus status = solver.Solve(model);

            if (status == CpSolverStatus.Optimal)
            {
                return new ScheduleOutput(status.ToString(), matrix.Select(dayModel => new DayOutput(dayModel.Select(assigned => solver.BooleanValue(assigned)).ToList())).ToList());
            }
            else
            {
                return new ScheduleOutput(status.ToString(), null);
            }
        }

        [Function("SolveSchedule_HttpStart")]
        public static async Task<HttpResponseData> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequestData req,
            [DurableClient] DurableTaskClient client,
            FunctionContext executionContext,
            [FromBody] ScheduleInput input)
        {
            ILogger logger = executionContext.GetLogger("SolveSchedule_HttpStart");

            // Function input comes from the request content.
            string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(
                nameof(SolveSchedule), input);

            logger.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

            // Returns an HTTP 202 response with an instance management payload.
            // See https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-http-api#start-orchestration
            return await client.CreateCheckStatusResponseAsync(req, instanceId);
        }

        // public record ScheduleInput(List<int> PeoplePerDay, List<List<bool>> PeopleAvailabiltyPerDay);
        public record ScheduleInput(List<DayInput> Days, List<PersonInput> People, int AssignmentWeight);

        public record DayInput(int MinimumAssigned, int MaximumAssigned, List<bool> AvailabilityPerPerson, List<int> DayPreferencePerPerson);

        public record PersonInput(int MinimumAssigned, int MaximumAssigned);

        public record ScheduleOutput(string Status, List<DayOutput>? DayOutputs);

        public record DayOutput(List<bool> PeopleAssigned);

        private static IEnumerable<List<T>> Transpose<T>(IEnumerable<IEnumerable<T>> list)
        {
            return
                //generate the list of top-level indices of transposed list
                Enumerable.Range(0, list.First().Count())
                //selects elements at list[y][x] for each x, for each y
                .Select(x => list.Select(y => y.ElementAt(x)).ToList());
        }
    }
}
