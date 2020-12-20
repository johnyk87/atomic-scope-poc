namespace atomic_scope
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Cassandra;

    public static class Program
    {
        public static async Task Main()
        {
            var scopeContext = new AtomicScopeContext();
            var repository1 = new CassandraRepository1(scopeContext);
            var repository2 = new CassandraRepository2(scopeContext);
            var scopeFactory = new CassandraAtomicScopeFactory();

            // These execute outside any scope.
            await repository1.DoSomethingAsync(Guid.NewGuid());
            await repository2.DoSomethingElseAsync(Guid.NewGuid());

            // These scopes are concurrent.
            var task1 = Task.Run(() =>
                ExecuteAtomicScopeAsync(scopeContext, scopeFactory, repository1, repository2));

            var task2 = Task.Run(() =>
                ExecuteAtomicScopeAsync(scopeContext, scopeFactory, repository1, repository2));

            await Task.WhenAll(task1, task2);
        }

        private static async Task ExecuteAtomicScopeAsync(
            AtomicScopeContext atomicScopeContext,
            IAtomicScopeFactory atomicScopeFactory,
            CassandraRepository1 repository1,
            CassandraRepository2 repository2)
        {
            atomicScopeContext.CurrentScope = atomicScopeFactory.Create();

            // These execute inside the scope.
            await CallRepositoriesAsync(repository1, repository2);

            await atomicScopeContext.CurrentScope.ApplyChangesAsync();
        }

        private static async Task CallRepositoriesAsync(
            CassandraRepository1 repository1,
            CassandraRepository2 repository2)
        {
            await repository1.DoSomethingAsync(Guid.NewGuid());
            var task1 = repository2.DoSomethingElseAsync(Guid.NewGuid());
            var task2 = Task.Run(() => repository1.DoSomethingAsync(Guid.NewGuid()));
            await Task.WhenAll(task1, task2);
        }
    }

    #region The test classes

    public abstract class BaseCassandraRepository
    {
        private readonly AtomicScopeContext atomicScopeContext;

        protected BaseCassandraRepository(AtomicScopeContext atomicScopeContext)
        {
            this.atomicScopeContext = atomicScopeContext;
        }

        protected async Task ExecuteWriteAsync(Statement statement)
        {
            var atomicScope = atomicScopeContext.CurrentScope;
            if (atomicScope is null)
            {
                Console.WriteLine("No atomic scope available.");
                Console.WriteLine($"Executing Cassandra statement: {statement}");
                await Task.Yield();
            }
            else
            {
                var cassandraScope = atomicScope.As<ICassandraAtomicScope>();
                if (cassandraScope is null)
                {
                    Console.WriteLine("No Cassandra scope available.");
                    Console.WriteLine($"Executing Cassandra statement: {statement}");
                    await Task.Yield();
                }
                else
                {
                    Console.WriteLine($"[{cassandraScope.GetHashCode()}] Adding Cassandra statement to atomic scope: {statement}.");
                    cassandraScope.AddStatement(statement);
                }
            }
        }
    }

    public class CassandraRepository1 : BaseCassandraRepository
    {
        public CassandraRepository1(AtomicScopeContext atomicScopeContext)
            : base(atomicScopeContext)
        {
        }

        public Task DoSomethingAsync(Guid id)
        {
            return base.ExecuteWriteAsync(new SimpleStatement($"insert into something (id) values ({id})"));
        }
    }

    public class CassandraRepository2 : BaseCassandraRepository
    {
        public CassandraRepository2(AtomicScopeContext atomicScopeContext)
            : base(atomicScopeContext)
        {
        }

        public Task DoSomethingElseAsync(Guid id)
        {
            return base.ExecuteWriteAsync(new SimpleStatement($"insert into something_else (id) values ({id})"));
        }
    }

    #endregion

    #region What really matters, part 1 - Abstractions

    public interface IAtomicScope
    {
        TScope As<TScope>();

        Task ApplyChangesAsync();
    }

    public class AtomicScopeContext
    {
        private readonly AsyncLocal<IAtomicScope> ScopeAcessor;

        public AtomicScopeContext()
        {
            this.ScopeAcessor = new AsyncLocal<IAtomicScope>();
        }

        public IAtomicScope CurrentScope { get => ScopeAcessor.Value; set { ScopeAcessor.Value = value; } }
    }

    /*
     * This abstraction isn't really needed to achieve the intended goals, but it is useful in
     * order to normalize and abstract the technology specific scope factories, making the
     * factories composable.
     */
    public interface IAtomicScopeFactory
    {
        IAtomicScope Create();
    }

    #endregion

    #region What really matters, part 2 - Cassandra implementation

    public class CassandraAtomicScopeFactory : IAtomicScopeFactory
    {
        public IAtomicScope Create()
        {
            return new CassandraAtomicScope();
        }
    }

    public interface ICassandraAtomicScope : IAtomicScope
    {
        void AddStatement(Statement statement);
        
        void AddStatements(IEnumerable<Statement> statements);
    }

    public class CassandraAtomicScope : ICassandraAtomicScope
    {
        private readonly ConcurrentBag<Statement> statements;

        public CassandraAtomicScope()
        {
            this.statements = new ConcurrentBag<Statement>();
            Console.WriteLine($"[{this.GetHashCode()}] Starting Cassandra atomic scope.");
        }

        public TScope As<TScope>()
        {
            return this is TScope intendedScope ? intendedScope : default;
        }

        public void AddStatement(Statement statement)
        {
            if (statement is null)
            {
                throw new ArgumentNullException(nameof(statement));
            }

            if (statement is BatchStatement)
            {
                throw new ArgumentException("Batch statements are not allowed in atomic operations.");
            }

            this.statements.Add(statement);
        }

        public void AddStatements(IEnumerable<Statement> statements)
        {
            if (statements is null)
            {
                throw new ArgumentNullException(nameof(statements));
            }

            foreach (var statement in statements)
            {
                this.AddStatement(statement);
            }
        }

        public async Task ApplyChangesAsync()
        {
            BatchStatement batchStatement;

            using (var statementsEnumerator = this.statements.GetEnumerator())
            {
                if (!statementsEnumerator.MoveNext())
                {
                    Console.WriteLine($"[{this.GetHashCode()}] No Cassandra changes to apply.");
                    return;
                }
                
                Console.WriteLine($"[{this.GetHashCode()}] Applying Cassandra changes in atomic scope:");

                Console.WriteLine($"\t[{this.GetHashCode()}] Preparing Cassandra batch statement:");
                batchStatement = new BatchStatement();

                do
                {
                    Console.WriteLine($"\t\t[{this.GetHashCode()}] {statementsEnumerator.Current}");
                    batchStatement.Add(statementsEnumerator.Current);
                }
                while (statementsEnumerator.MoveNext());
            }

            Console.WriteLine($"\t[{this.GetHashCode()}] Executing Cassandra batch statement.");
            await Task.Yield();
        }
    }

    #endregion
}
