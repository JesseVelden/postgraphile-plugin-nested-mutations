const { graphql } = require('graphql');
const { withSchema } = require('../helpers');

test(
  'ignore conflict when create is nested in upsert',
  withSchema({
    setup: `
      create table p.parent (
        id serial primary key,
        name text not null
      );

      create table p.child (
        id serial primary key,
        parent_id integer,
        name text not null,
        constraint child_parent_fkey foreign key (parent_id)
          references p.parent (id)
      );
      insert into p.parent values(1, 'test');
      insert into p.child values(99, 1, 'test child');
    `,
    test: async ({ schema, pgClient }) => {
      const query = `
        mutation {
          updateParentById(
            input: {
              id: 1
              parentPatch: {
                childrenUsingId: {
                  upsert: [{
                    create: { id: 99, name: "test child conflict" },
                  }]
                }
              }
            }
          ) {
            parent {
              id
              name
              childrenByParentId {
                nodes {
                  id
                  parentId
                  name
                }
              }
            }
          }
        }
      `;
      expect(schema).toMatchSnapshot();

      const result = await graphql(schema, query, null, { pgClient });
      expect(result).not.toHaveProperty('errors');

      const data = result.data.updateParentById.parent;
      expect(data.childrenByParentId.nodes).toHaveLength(1);
      data.childrenByParentId.nodes.map((n) =>
        expect(n.parentId).toBe(data.id),
      );
    },
  }),
);

test(
  "deleteOthers removes everything that isn't connected or created in upsert",
  withSchema({
    setup: `
      create table p.parent (
        id serial primary key,
        name text not null
      );

      create table p.child (
        id serial primary key,
        parent_id integer,
        name text not null,
        constraint child_parent_fkey foreign key (parent_id)
          references p.parent (id)
      );
      insert into p.parent values(1, 'test');
      insert into p.child values(99, 1, 'child kept');
      insert into p.child values(100, 1, 'child that will be removed');
    `,
    test: async ({ schema, pgClient }) => {
      const query = `
        mutation {
          updateParentById(
            input: {
              id: 1
              parentPatch: {
                childrenUsingId: {
                  deleteOthers: true,
                  connectById: { id: 99 },
                  upsert: [{
                    create: { id: 101, name: "new child" },
                  }]
                }
              }
            }
          ) {
            parent {
              id
              name
              childrenByParentId {
                nodes {
                  id
                  parentId
                  name
                }
              }
            }
          }
        }
      `;
      expect(schema).toMatchSnapshot();

      const result = await graphql(schema, query, null, { pgClient });
      expect(result).not.toHaveProperty('errors');

      const data = result.data.updateParentById.parent;
      expect(data.childrenByParentId.nodes).toHaveLength(2);
      data.childrenByParentId.nodes.map((n) =>
        expect(n.parentId).toBe(data.id),
      );
    },
  }),
);
