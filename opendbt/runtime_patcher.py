import importlib
from typing import Callable, Type, Any
from opendbt.logger import OpenDbtLogger


class RuntimePatcher(OpenDbtLogger):
    """
    A utility class for patching modules and classes at runtime.

    This class provides a simplified way to replace existing functions,
    classes, or attributes within modules with custom implementations.
    """

    def __init__(self, module_name: str):
        """
        Initializes the RuntimePatcher for a specific module.

        Args:
            module_name: The name of the module to patch (e.g., "dbt.config").
        """
        self.module_name = module_name
        self.module = importlib.import_module(module_name)

    def patch_function(self, function_name: str, new_function: Callable):
        """
        Patches a function within the module.

        Args:
            function_name: The name of the function to patch.
            new_function: The new function to use as a replacement.
        """
        setattr(self.module, function_name, new_function)
        self.log.debug(f"Patched function: {self.module_name}.{function_name}")

    def patch_class(self, class_name: str, new_class: Type):
        """
        Patches a class within the module.

        Args:
            class_name: The name of the class to patch.
            new_class: The new class to use as a replacement.
        """
        setattr(self.module, class_name, new_class)
        self.log.debug(f"Patched class: {self.module_name}.{class_name}")

    def patch_attribute(self, attribute_name: str, new_value: Any):
        """
        Patches an attribute within the module.

        Args:
            attribute_name: The name of the attribute to patch.
            new_value: The new value to assign to the attribute.
        """
        setattr(self.module, attribute_name, new_value)
        self.log.debug(f"Patched attribute: {self.module_name}.{attribute_name}")

    def patch_class_method(self, class_name: str, method_name: str, new_method: Callable):
        """
        Patches a class method within the module.

        Args:
            class_name: The name of the class containing the method.
            method_name: The name of the method to patch.
            new_method: The new method to use as a replacement.
        """
        target_class = getattr(self.module, class_name)
        setattr(target_class, method_name, new_method)
        self.log.debug(f"Patched class method: {self.module_name}.{class_name}.{method_name}")


class _PatchDecorator:
    """
    Base class for patch decorators
    """

    def __init__(self, module_name: str, target_name: str):
        self.module_name = module_name
        self.target_name = target_name
        self.patcher = RuntimePatcher(self.module_name)


class PatchClass(_PatchDecorator):
    """
    A decorator for patching classes at runtime.
    """

    def __call__(self, target: Type):
        self.patcher.patch_class(self.target_name, target)
        return target


class PatchFunction(_PatchDecorator):
    """
    A decorator for patching functions at runtime.
    """

    def __call__(self, target: Callable):
        self.patcher.patch_function(self.target_name, target)
        return target


class PatchAttribute(_PatchDecorator):
    """
    A decorator for patching attributes at runtime.
    """

    def __call__(self, target: Any):
        # if it is callable, call it to get the value
        if callable(target):
            target = target()
        self.patcher.patch_attribute(self.target_name, target)
        return target


class PatchClassMethod(_PatchDecorator):
    """
    A decorator for patching class methods at runtime.
    """

    def __init__(self, module_name: str, class_name: str, method_name: str):
        super().__init__(module_name, class_name)
        self.method_name = method_name

    def __call__(self, target: Callable):
        self.patcher.patch_class_method(self.target_name, self.method_name, target)
        return target

# Example Usage:

# Example to use PatchClass for override the ModelRunner class
# @PatchClass(module_name="dbt.task.run", target_name="ModelRunner")
# class CustomModelRunner:
#     def __init__(self, *args, **kwargs):
#         print("Custom ModelRunner initialized!")
#
#
# # Example to use PatchClass for override the RuntimeConfig class
# @PatchClass(module_name="dbt.config", target_name="RuntimeConfig")
# class CustomRuntimeConfig:
#     def __init__(self, *args, **kwargs):
#         print("Custom RuntimeConfig initialized!")
#
# # Example to use PatchAttribute for override the FACTORY attribute
# @PatchAttribute(module_name="dbt.adapters.factory", target_name="FACTORY")
# def get_custom_open_dbt_adapter_container():
#     class CustomOpenDbtAdapterContainer:
#         def __init__(self, *args, **kwargs):
#             print("Custom OpenDbtAdapterContainer initialized!")
#     return CustomOpenDbtAdapterContainer
#
#
# # Example to use PatchFunction for override the sqlfluff_lint function
# @PatchFunction(module_name="dbt.cli.main", target_name="sqlfluff_lint")
# def custom_sqlfluff_lint():
#     print("Custom sqlfluff_lint called!")

# Example to patch class method
# @PatchClassMethod(module_name="dbt.adapters.factory", class_name="AdapterContainer", method_name="get_adapter")
# def custom_get_adapter(self, *args, **kwargs):
#     print("Custom get_adapter method called!")
#     return "Custom Adapter"
