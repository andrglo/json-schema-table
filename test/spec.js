import assert from 'assert';
import JsonSchemaTable from '../lib';

export default function(db) {

  describe('create table', function() {
    it('should have unit test!', function(done) {
      let person = new JsonSchemaTable('person', require('./schemas/person.json'),
        {db: db});

      person.sync();

      //console.log('create', user.create().toQuery())
//
//    var post = sql.define({
//      name: 'post',
//      columns: ['id', 'userId', 'date', 'title', 'body']
//    });
//
////now let's make a simple query
//    var query = user.select(user.star()).from(user).toQuery();
//    console.log(query.text); //SELECT "user".* FROM "user"
//
////something more interesting
//    query = user
//      .select(user.id)
//      .from(user)
//      .where(
//      user.name.equals('boom').and(user.id.equals(1))
//    ).or(
//      user.name.equals('bang').and(user.id.equals(2))
//    ).toQuery();
//
////query is parameterized by default
//    console.log(query.text); //SELECT "user"."id" FROM "user" WHERE ((("user"."name" = $1) AND ("user"."id" = $2)) OR (("user"."name" = $3) AND ("user"."id" = $4)))
//
//    console.log(query.values); //['boom', 1, 'bang', 2]
//
////queries can be named
//    query = user.select(user.star()).from(user).toNamedQuery('user.all');
//    console.log(query.name); //'user.all'
//
////how about a join?
//    query = user.select(user.name, post.body)
//      .from(user.join(post).on(user.id.equals(post.userId))).toQuery();
//
//    console.log(query.text); //'SELECT "user"."name", "post"."body" FROM "user" INNER JOIN "post" ON ("user"."id" = "post"."userId")'
//
////this also makes parts of your queries composable, which is handy
//
//    var friendship = sql.define({
//      name: 'friendship',
//      columns: ['userId', 'friendId']
//    });
//
//    var friends = user.as('friends');
//    var userToFriends = user
//      .leftJoin(friendship).on(user.id.equals(friendship.userId))
//      .leftJoin(friends).on(friendship.friendId.equals(friends.id));
//
////and now...compose...
//    var friendsWhoHaveLoggedInQuery = user.from(userToFriends).where(friends.lastLogin.isNotNull());
////SELECT * FROM "user"
////LEFT JOIN "friendship" ON ("user"."id" = "friendship"."userId")
////LEFT JOIN "user" AS "friends" ON ("friendship"."friendId" = "friends"."id")
////WHERE "friends"."lastLogin" IS NOT NULL
//
//    var friendsWhoUseGmailQuery = user.from(userToFriends).where(friends.email.like('%@gmail.com'));
////SELECT * FROM "user"
////LEFT JOIN "friendship" ON ("user"."id" = "friendship"."userId")
////LEFT JOIN "user" AS "friends" ON ("friendship"."friendId" = "friends"."id")
////WHERE "friends"."email" LIKE %1
//
////Using different property names for columns
////helpful if your column name is long or not camelCase
//    user = sql.define({
//      name: 'user',
//      columns: [{
//        name: 'id'
//      }, {
//        name: 'state_or_province',
//        property: 'state'
//      }
//      ]
//    });
//
////now, instead of user.state_or_province, you can just use user.state
//    console.log(user.select().where(user.state.equals('WA')).toQuery().text);
//// "SELECT "user".* FROM "user" WHERE ("user"."state_or_province" = $1)"

      assert(true, 'we expected this package author to add actual unit tests.');
      done();
    });
  });
}
