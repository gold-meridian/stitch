/*
 * Copyright (c) 2016, 2017, 2018, 2019 FabricMC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.fabricmc.stitch.commands.tinyv2;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import net.fabricmc.stitch.Command;
import net.fabricmc.stitch.util.Pair;

/**
 * Merges a tiny file with 2 columns (namespaces) of mappings, with another tiny file that has
 * the same namespace as the first column and a different namespace as the second column.
 * The first column of the output will contain the shared namespace,
 * the second column of the output would be the second namespace of input a,
 * and the third column of the output would be the second namespace of input b
 * <p>
 * Descriptors will remain as-is (using the namespace of the first column)
 * <p>
 * <p>
 * For example:
 * <p>
 * Input A:
 * intermediary                 named
 * c    net/minecraft/class_123      net/minecraft/somePackage/someClass
 * m   (Lnet/minecraft/class_124;)V  method_1234 someMethod
 * <p>
 * Input B:
 * intermediary                 official
 * c    net/minecraft/class_123      a
 * m   (Lnet/minecraft/class_124;)V  method_1234 a
 * <p>
 * The output will be:
 * <p>
 * intermediary                 named                                  official
 * c    net/minecraft/class_123      net/minecraft/somePackage/someClass    a
 * m   (Lnet/minecraft/class_124;)V  method_1234 someMethod    a
 * <p>
 * <p>
 * After intermediary-named mappings are obtained,
 * and official-intermediary mappings are obtained and swapped using CommandReorderTinyV2, Loom merges them using this command,
 * and then reorders it to official-intermediary-named using CommandReorderTinyV2 again.
 * This is a convenient way of storing all the mappings in Loom.
 */
public class CommandMergeTinyV2 extends Command {
	private static final TinyClass EMPTY_CLASS = new TinyClass(Collections.emptyList());
	private static final TinyMethod EMPTY_METHOD = new TinyMethod(null, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

	public CommandMergeTinyV2() {
		super("mergeTinyV2");
	}

	private static <T> List<T> union(Stream<T> list1, Stream<T> list2) {
		return union(list1.collect(Collectors.toList()), list2.collect(Collectors.toList()));
	}

	private static <T> List<T> union(Collection<T> list1, Collection<T> list2) {
		Set<T> set = new HashSet<T>();

		set.addAll(list1);
		set.addAll(list2);

		return new ArrayList<T>(set);
	}

	private static String escape(String str) {
		return Pattern.quote(str);
	}

	private static <S, E> List<E> map(List<S> from, Function<S, E> mapper) {
		return from.stream().map(mapper).collect(Collectors.toList());
	}

	/**
	 * <input-a> and <input-b> are the tiny files to be merged. The result will be written to <output>.
	 */
	@Override
	public String getHelpString() {
		return "<input-a> <input-b> <output> [-c|--common-namespace <namespace>] [-h|--leave-holes]";
	}

	@Override
	public boolean isArgumentCountValid(int count) {
		return count >= 3;
	}

	@Override
	public void run(String[] args) throws IOException {
		String commonNamespace = null;
		boolean leaveHoles = false;
		for (int i = 3; i < args.length; i++) {
			switch (args[i].toLowerCase(Locale.ROOT)) {
				case "-c":
				case "--common-namespace":
					commonNamespace = args[++i];
					break;

				case "-h":
				case "--leave-holes":
					leaveHoles = true;
					break;
			}
		}

		Path inputA = Paths.get(args[0]);
		Path inputB = Paths.get(args[1]);
		System.out.println("Reading " + inputA);
		TinyFile tinyFileA = TinyV2Reader.read(inputA);
		System.out.println("Reading " + inputB);
		TinyFile tinyFileB = TinyV2Reader.read(inputB);
		System.out.println("Merging " + inputA + " with " + inputB);
		TinyFile mergedFile = merge(tinyFileA, tinyFileB, commonNamespace, leaveHoles);

		TinyV2Writer.write(mergedFile, Paths.get(args[2]));
		System.out.println("Merged mappings written to " + Paths.get(args[2]));
	}

	private TinyFile merge(TinyFile inputA, TinyFile inputB, @Nullable String commonNamespace, boolean leaveHoles) {
		TinyHeader headerA = inputA.getHeader();
		TinyHeader headerB = inputB.getHeader();

		if (commonNamespace == null) {
			List<String> namespaces = new ArrayList<>(headerA.getNamespaces());
			namespaces.retainAll(headerB.getNamespaces());

			switch (namespaces.size()) {
				case 0:
					throw new IllegalArgumentException("No common namespaces between inputs, only found A: " + headerA.getNamespaces() + ", B: " + headerB.getNamespaces());

				case 1:
					commonNamespace = namespaces.get(0);
					break;

				default:
					throw new IllegalArgumentException("Multiple common namespaces between inputs: " + namespaces + ", specify the desired common namespace via -c");
			}
		} else {
			if (!headerA.getNamespaces().contains(commonNamespace)) {
				throw new IllegalArgumentException("Unable to find specified common namespace in A input, only found " + headerA.getNamespaces());
			}
			if (!headerB.getNamespaces().contains(commonNamespace)) {
				throw new IllegalArgumentException("Unable to find specified common namespace in B input, only found " + headerB.getNamespaces());
			}
		}

		Set<String> newNamespaces = new LinkedHashSet<>(headerA.getNamespaces());
		newNamespaces.addAll(headerB.getNamespaces());

		//TODO: how to merge properties?

		TinyHeader mergedHeader = mergeHeaders(headerA, headerB, newNamespaces);

		MergeContext mergeContext = new MergeContext(
				commonNamespace,
				headerA.getNamespaces().indexOf(commonNamespace),
				headerB.getNamespaces().indexOf(commonNamespace),
				leaveHoles,
				newNamespaces,
				newNamespaces.stream().collect(Collectors.toMap(x -> x, x -> headerA.getNamespaces().indexOf(x))),
				newNamespaces.stream().collect(Collectors.toMap(x -> x, x -> headerB.getNamespaces().indexOf(x)))
		);

		List<String> keyUnion = keyUnion(inputA.getClassEntries(), inputB.getClassEntries(), mergeContext.commonNamespaceA, mergeContext.commonNamespaceB);

		Map<String, TinyClass> inputAClasses = inputA.mapClassesByNamespace(mergeContext.commonNamespaceA);
		Map<String, TinyClass> inputBClasses = inputB.mapClassesByNamespace(mergeContext.commonNamespaceB);
		List<TinyClass> mergedClasses = map(keyUnion, key -> {
			TinyClass classA = inputAClasses.get(key);
			TinyClass classB = inputBClasses.get(key);

			classA = matchEnclosingClassIfNeeded(key, classA, inputAClasses);
			classB = matchEnclosingClassIfNeeded(key, classB, inputBClasses);
			return mergeClasses(key, classA, classB, mergeContext);
		});

		return new TinyFile(mergedHeader, mergedClasses);
	}

	private TinyClass matchEnclosingClassIfNeeded(String key, TinyClass tinyClass, Map<String, TinyClass> mappings) {
		if (tinyClass == null && key.contains("$")) {
			String partlyMatchedClassName = matchEnclosingClass(key, mappings);
			return new TinyClass(Arrays.asList(key, partlyMatchedClassName));
		} else {
			return tinyClass;
		}
	}

	/**
	 * Takes something like net/minecraft/class_123$class_124 that doesn't have a mapping, tries to find net/minecraft/class_123
	 * , say the mapping of net/minecraft/class_123 is path/to/someclass and then returns a class of the form
	 * path/to/someclass$class124
	 */
	@Nonnull
	private String matchEnclosingClass(String sharedName, Map<String, TinyClass> inputBClassBySharedNamespace) {
		String[] path = sharedName.split(escape("$"));
		int parts = path.length;
		for (int i = parts - 2; i >= 0; i--) {
			String currentPath = String.join("$", Arrays.copyOfRange(path, 0, i + 1));
			TinyClass match = inputBClassBySharedNamespace.get(currentPath);

			if (match != null && !match.getClassNames().get(1).isEmpty()) {
				return match.getClassNames().get(1)
						+ "$" + String.join("$", Arrays.copyOfRange(path, i + 1, path.length));

			}
		}

		return sharedName;
	}

	private TinyClass mergeClasses(String sharedClassName, @Nullable TinyClass classA, @Nullable TinyClass classB, MergeContext mergeContext) {
		List<String> mergedNames = mergeNames(sharedClassName, classA, classB, mergeContext);
		if (classA == null) classA = EMPTY_CLASS;
		if (classB == null) classB = EMPTY_CLASS;
		List<String> mergedComments = mergeComments(classA.getComments(), classB.getComments());

		List<Pair<String, String>> methodKeyUnion = union(mapToNamespaceAndDescriptor(classA, mergeContext.commonNamespaceA), mapToNamespaceAndDescriptor(classB, mergeContext.commonNamespaceB));
		Map<Pair<String, String>, TinyMethod> methodsA = classA.mapMethodsByNamespaceAndDescriptor(mergeContext.commonNamespaceA);
		Map<Pair<String, String>, TinyMethod> methodsB = classB.mapMethodsByNamespaceAndDescriptor(mergeContext.commonNamespaceB);
		List<TinyMethod> mergedMethods = map(methodKeyUnion,
				(Pair<String, String> k) -> mergeMethods(k.getLeft(), methodsA.get(k), methodsB.get(k), mergeContext));

		List<String> fieldKeyUnion = keyUnion(classA.getFields(), classB.getFields(), mergeContext.commonNamespaceA, mergeContext.commonNamespaceB);
		Map<String, TinyField> fieldsA = classA.mapFieldsByNamespace(mergeContext.commonNamespaceA);
		Map<String, TinyField> fieldsB = classB.mapFieldsByNamespace(mergeContext.commonNamespaceB);
		List<TinyField> mergedFields = map(fieldKeyUnion, k -> mergeFields(k, fieldsA.get(k), fieldsB.get(k), mergeContext));

		return new TinyClass(mergedNames, mergedMethods, mergedFields, mergedComments);
	}

	private TinyMethod mergeMethods(String sharedMethodName, @Nullable TinyMethod methodA, @Nullable TinyMethod methodB, MergeContext mergeContext) {
		List<String> mergedNames = mergeNames(sharedMethodName, methodA, methodB, mergeContext);
		if (methodA == null) methodA = EMPTY_METHOD;
		if (methodB == null) methodB = EMPTY_METHOD;
		List<String> mergedComments = mergeComments(methodA.getComments(), methodB.getComments());

		// TODO I guess common namespace is impossible here?
		String descriptor = methodA.getMethodDescriptorInFirstNamespace() != null ? methodA.getMethodDescriptorInFirstNamespace()
				: methodB.getMethodDescriptorInFirstNamespace();
		if (descriptor == null) throw new RuntimeException("no descriptor for key " + sharedMethodName);


		List<Integer> parametersKeyUnion = union(methodA.getParameters().stream().map(TinyMethodParameter::getLvIndex), methodB.getParameters().stream().map(TinyMethodParameter::getLvIndex));
		Map<Integer, TinyMethodParameter> parametersA = methodA.mapParametersByLvIndex();
		Map<Integer, TinyMethodParameter> parametersB = methodB.mapParametersByLvIndex();
		List<TinyMethodParameter> mergedParameters = map(parametersKeyUnion, k -> mergeParameters(k, parametersA.get(k), parametersB.get(k), mergeContext));

		List<Integer> localVariablesKeyUnion = union(methodA.getLocalVariables().stream().map(TinyLocalVariable::getLvIndex), methodB.getLocalVariables().stream().map(TinyLocalVariable::getLvIndex));
		Map<Integer, TinyLocalVariable> localVariablesA = methodA.mapLocalVariablesByLvIndex();
		Map<Integer, TinyLocalVariable> localVariablesB = methodB.mapLocalVariablesByLvIndex();
		List<TinyLocalVariable> mergedLocalVariables = map(localVariablesKeyUnion, k -> mergeLocalVariables(k, localVariablesA.get(k), localVariablesB.get(k), mergeContext));

		return new TinyMethod(descriptor, mergedNames, mergedParameters, mergedLocalVariables, mergedComments);
	}

	private TinyMethodParameter mergeParameters(int lvIndex, @Nullable TinyMethodParameter parameterA, @Nullable TinyMethodParameter parameterB, MergeContext mergeContext) {
		List<String> names = mergeNames(null, parameterA, parameterB, mergeContext);

		List<String> comments = mergeComments(
				parameterA == null ? Collections.emptyList() : parameterA.getComments(),
				parameterB == null ? Collections.emptyList() : parameterB.getComments()
		);

		return new TinyMethodParameter(lvIndex, names, comments);
	}

	private TinyLocalVariable mergeLocalVariables(int lvIndex, @Nullable TinyLocalVariable localVariableA, @Nullable TinyLocalVariable localVariableB, MergeContext mergeContext) {
		List<String> names = mergeNames(null, localVariableA, localVariableB, mergeContext);

		List<String> comments = mergeComments(
				localVariableA == null ? Collections.emptyList() : localVariableA.getComments(),
				localVariableB == null ? Collections.emptyList() : localVariableB.getComments()
		);

		int lvStartOffset = localVariableA != null ? localVariableA.getLvStartOffset() : Objects.requireNonNull(localVariableB).getLvStartOffset();
		int lvTableIndex = localVariableA != null ? localVariableA.getLvTableIndex() : Objects.requireNonNull(localVariableB).getLvTableIndex();

		return new TinyLocalVariable(lvIndex, lvStartOffset, lvTableIndex, names, comments);
	}

	private TinyField mergeFields(String sharedFieldName, @Nullable TinyField fieldA, @Nullable TinyField fieldB, MergeContext mergeContext) {
		List<String> mergedNames = mergeNames(sharedFieldName, fieldA, fieldB, mergeContext);
		List<String> mergedComments = mergeComments(fieldA != null ? fieldA.getComments() : Collections.emptyList(),
				fieldB != null ? fieldB.getComments() : Collections.emptyList());

		// TODO ditto
		String descriptor = fieldA != null ? fieldA.getFieldDescriptorInFirstNamespace()
				: fieldB != null ? fieldB.getFieldDescriptorInFirstNamespace() : null;
		if (descriptor == null) throw new RuntimeException("no descriptor for key " + sharedFieldName);

		return new TinyField(descriptor, mergedNames, mergedComments);
	}

	private TinyHeader mergeHeaders(TinyHeader headerA, TinyHeader headerB, Set<String> extraNamespaces) {
		// TODO: how should versions and properties be merged?
		return new TinyHeader(new ArrayList<>(extraNamespaces), headerA.getMajorVersion(), headerA.getMinorVersion(), headerA.getProperties());
	}

	private List<String> mergeComments(Collection<String> commentsA, Collection<String> commentsB) {
		return union(commentsA, commentsB);
	}

	private <T extends Mapping> List<String> keyUnion(Collection<T> mappingsA, Collection<T> mappingB, int commonNamespaceA, int commonNamespaceB) {
		return union(mappingsA.stream().map(m -> m.getMapping().get(commonNamespaceA)), mappingB.stream().map(m -> m.getMapping().get(commonNamespaceB)));
	}

	private Stream<Pair<String, String>> mapToNamespaceAndDescriptor(TinyClass tinyClass, int namespace) {
		// TODO ditto
		return tinyClass.getMethods().stream().map(m -> Pair.of(m.getMapping().get(namespace), m.getMethodDescriptorInFirstNamespace()));
	}

	private List<String> mergeNames(@Nullable String key, @Nullable Mapping mappingA, @Nullable Mapping mappingB, MergeContext mergeContext) {
		List<String> merged = new ArrayList<>();

		for (String newNamespace : mergeContext.newNamespaces) {
			if (key != null && newNamespace.equals(mergeContext.commonNamespace)) {
				merged.add(key);
				continue;
			}

			if (mappingA != null) {
				Integer namespaceA = mergeContext.namespaceMapA.get(newNamespace);
				if (namespaceA != -1) {
					String nameA = mappingA.getMapping().get(namespaceA);
					if (!nameA.isEmpty()) {
						merged.add(nameA);
						continue;
					}
				}
			}

			if (mappingB != null) {
				Integer namespaceB = mergeContext.namespaceMapB.get(newNamespace);
				if (namespaceB != -1) {
					String nameB = mappingB.getMapping().get(namespaceB);
					if (!nameB.isEmpty()) {
						merged.add(nameB);
						continue;
					}
				}
			}

			merged.add(mergeContext.leaveHoles || key == null ? "" : key);
		}

		return merged;
	}

	private static class MergeContext {
		public String commonNamespace;
		public int commonNamespaceA;
		public int commonNamespaceB;
		public boolean leaveHoles;
		public Set<String> newNamespaces;
		public Map<String, Integer> namespaceMapA;
		public Map<String, Integer> namespaceMapB;

		public MergeContext(String commonNamespace, int commonNamespaceA, int commonNamespaceB, boolean leaveHoles, Set<String> newNamespaces, Map<String, Integer> namespaceMapA, Map<String, Integer> namespaceMapB) {
			this.commonNamespace = commonNamespace;
			this.commonNamespaceA = commonNamespaceA;
			this.commonNamespaceB = commonNamespaceB;
			this.leaveHoles = leaveHoles;
			this.newNamespaces = newNamespaces;
			this.namespaceMapA = namespaceMapA;
			this.namespaceMapB = namespaceMapB;
		}
	}

}