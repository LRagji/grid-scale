1. Single Responsibility Principle (SRP)
Each class should have only one responsibility or reason to change. This makes classes easier to understand, maintain, and test.

2. Open/Closed Principle (OCP)
Classes should be open for extension but closed for modification. This means you should be able to add new functionality without changing existing code, often achieved through inheritance or interfaces.

3. Liskov Substitution Principle (LSP)
Subtypes must be substitutable for their base types. This ensures that derived classes extend the base class without changing its behavior.

4. Interface Segregation Principle (ISP)
Clients should not be forced to depend on interfaces they do not use. Split large interfaces into smaller, more specific ones.

5. Dependency Inversion Principle (DIP)
Depend on abstractions, not on concrete implementations. This can be achieved using interfaces or abstract classes.

6. Encapsulation
Keep the internal state of an object hidden and only expose necessary methods. Use private or protected access modifiers to restrict access to internal data.

7. Composition over Inheritance
Favor composition over inheritance to achieve code reuse. This means using objects within other objects to achieve functionality rather than inheriting from a base class.

8. Cohesion
Ensure that the methods and properties of a class are closely related to its purpose. High cohesion within a class makes it more understandable and maintainable.

9. Coupling
Aim for low coupling between classes. This means that changes in one class should have minimal impact on other classes. Use interfaces and dependency injection to reduce coupling.

10. Naming Conventions
Use meaningful and descriptive names for classes, methods, and properties. This improves code readability and maintainability.

10. Naming Conventions
Use meaningful and descriptive names for classes, methods, and properties. This improves code readability and maintainability.

11. Avoid God Objects
Do not create classes that try to do too much. These "God objects" are difficult to maintain and understand.

12. Use Design Patterns
Leverage established design patterns (e.g., Singleton, Factory, Observer) to solve common design problems. This can lead to more robust and maintainable code.

13. Document Your Code
Provide clear documentation for your classes and methods. This helps other developers understand the purpose and usage of your code.

14. Testability
Design classes in a way that makes them easy to test. This often involves using dependency injection and avoiding static methods.