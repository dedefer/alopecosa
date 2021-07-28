box.cfg{listen=3301}

function test(num)
  return num, num+1
end

test_space = box.schema.space.create('test', {
  id = 512, format = {
    {name = 'id', type = 'unsigned'},
    {name = 'value1', type = 'unsigned'},
    {name = 'value2', type = 'unsigned'},
}})

test_space:create_index('primary', {
  unique = true, parts = {'id'},
})

test_space:insert{1, 2, 3}

box.schema.user.create('em', {password='em'})
box.schema.user.grant('em', 'execute', 'universe')
