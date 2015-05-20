using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.CodeDom;
using Orleans;

using Orleans.Serialization;

namespace GrainClientGenerator.Serialization
{
    class SerializerGenerationUtilities
    {
        static internal CodeMemberMethod GenerateCopier(string name, string typeName, CodeTypeParameterCollection genericTypeParams = null)
        {
            var copier = new CodeMemberMethod() { Name = name };
            //if (genericTypeParams != null)
            //{
            //    copier.TypeParameters.AddRange(genericTypeParams);
            //}
            copier.Attributes = (copier.Attributes & ~MemberAttributes.AccessMask) | MemberAttributes.Public;
            copier.Attributes = (copier.Attributes & ~MemberAttributes.ScopeMask) | MemberAttributes.Static;
            copier.CustomAttributes.Add(new CodeAttributeDeclaration(new CodeTypeReference(typeof(Orleans.CopierMethodAttribute))));
            copier.ReturnType = new CodeTypeReference(typeof(object));
            copier.Parameters.Add(new CodeParameterDeclarationExpression(typeof(object), "original"));
            copier.Statements.Add(new CodeVariableDeclarationStatement(typeName, "input", new CodeCastExpression(typeName, new CodeArgumentReferenceExpression("original"))));
            return copier;
        }

        static internal CodeMemberMethod GenerateSerializer(string name, string typeName, CodeTypeParameterCollection genericTypeParams = null)
        {
            var serializer = new CodeMemberMethod() { Name = name };
            //if (genericTypeParams != null)
            //{
            //    serializer.TypeParameters.AddRange(genericTypeParams);
            //}
            serializer.Attributes = (serializer.Attributes & ~MemberAttributes.AccessMask) | MemberAttributes.Public;
            serializer.Attributes = (serializer.Attributes & ~MemberAttributes.ScopeMask) | MemberAttributes.Static;
            serializer.CustomAttributes.Add(new CodeAttributeDeclaration(new CodeTypeReference(typeof(SerializerMethodAttribute))));
            serializer.ReturnType = new CodeTypeReference(typeof(void));
            serializer.Parameters.Add(new CodeParameterDeclarationExpression(typeof(object), "original"));
            serializer.Parameters.Add(new CodeParameterDeclarationExpression(typeof(BinaryTokenStreamWriter), "stream"));
            serializer.Parameters.Add(new CodeParameterDeclarationExpression(typeof(Type), "expected"));
            serializer.Statements.Add(new CodeVariableDeclarationStatement(typeName, "input", new CodeCastExpression(typeName, new CodeArgumentReferenceExpression("original"))));
            return serializer;
        }

        static internal CodeMemberMethod GenerateDeserializer(string name, string typeName, CodeTypeParameterCollection genericTypeParams = null)
        {
            var deserializer = new CodeMemberMethod() { Name = name };
            //if (genericTypeParams != null)
            //{
            //    deserializer.TypeParameters.AddRange(genericTypeParams);
            //}
            deserializer.Attributes = (deserializer.Attributes & ~MemberAttributes.AccessMask) | MemberAttributes.Public;
            deserializer.Attributes = (deserializer.Attributes & ~MemberAttributes.ScopeMask) | MemberAttributes.Static;
            deserializer.CustomAttributes.Add(new CodeAttributeDeclaration(new CodeTypeReference(typeof(DeserializerMethodAttribute))));
            deserializer.ReturnType = new CodeTypeReference(typeof(object));
            deserializer.Parameters.Add(new CodeParameterDeclarationExpression(typeof(Type), "expected"));
            deserializer.Parameters.Add(new CodeParameterDeclarationExpression(typeof(BinaryTokenStreamReader), "stream"));
            return deserializer;
        }
    }
}
