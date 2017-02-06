package M;
use Mouse;
has 'nodes' => ( is => 'ro', isa => 'ArrayRef');
has 'names' => (is => 'ro', isa => 'ArrayRef');
has 'shard_count' => (is => 'ro', isa => 'Int' );
has 'space' => (is => 'ro', isa => 'Int');
has 'indexes' => (is => 'ro', isa => 'ArrayRef');
has 'debug' => (is => 'ro', isa => 'Int');



sub _nodes_names {
    my ($self) = @_;
    my $return = {};
    my $i = 0;
    die "wrong data" unless (scalar(@{$self->nodes}) == scalar(@{$self->names}));
    for (@{$self->nodes}) {
        $return->{$_} = $self->names->[$i];
        $i++;
    }
    return $return;
}

sub shards_drop {
    my $self = shift;
    for (@{$self->nodes}){
        print "DELETE FROM NODE $_$/";
        my $s = $_;
        for (1 .. $self->shard_count) {
            print "./shardbox.pl -s=$s shard $_ delete";
            system("./shardbox.pl -s=$s shard $_ delete") unless $self->debug;
        }
    }
}

#sub drop_space {
#    my $self = shift;
    #shardbox.pl -s=<HOST> shard <SID> obj_space <OID> create [no_snap] [no_wal] <INDEX CONF>;
#    my $s = $self->nodes->[0];
#    my $space = $self->space;
#    for (1 .. $self->shard_count) {
#        print "./shardbox.pl -s=$s shard $_ obj_space $space drop$/";
#        system("./shardbox.pl -s=$s shard $_ obj_space $space drop");
#    }
    #shardbox.pl -s=<HOST> shard <SID> obj_space <OID> truncate

#}
sub drop_spaces {
    my $self = shift;
    my $space = $self->space;
    my $s_per_node = int($self->shard_count / scalar (@{$self->nodes}));
    my $s_residue = $self->shard_count % scalar (@{$self->nodes});
    my $current = 1;
    my $nodes_names = $self->_nodes_names();
    for my $node (@{$self->nodes}) {
        my $count = $s_per_node;
        if ($s_residue) {
            $count++;
            $s_residue--;
        }
        my $save_node_name = delete $nodes_names->{$node};
        for my $shard ($current..$current + $count - 1){
            my $replicas = join ' ', values %$nodes_names;
            my $st = "./shardbox.pl -s=$node shard $shard obj_space $space drop";
            print $st;;
            system $st unless $self->debug;
        }
        $nodes_names->{$node} = $save_node_name;
        $current+= $count;
        print "===================================";
    }
}

sub truncate_space {
    my $self = shift;
    #shardbox.pl -s=<HOST> shard <SID> obj_space <OID> create [no_snap] [no_wal] <INDEX CONF>;
    my $s = $self->nodes->[0];
    my $space = $self->space;
    for (1 .. $self->shard_count) {
        print "./shardbox.pl -s=$s shard $_ obj_space $space truncate$/";
        system("./shardbox.pl -s=$s shard $_ obj_space $space truncate") unless $self->debug;
    }
    #shardbox.pl -s=<HOST> shard <SID> obj_space <OID> truncate

}
sub drop_indexes {
    my $self = shift;
    my $s = $self->nodes->[0];
    my $space = $self->space;
    for my $shard (1..$self->shard_count) {
        for ( my $i = 0; $i < @{$self->indexes}; $i++){
            my $command = "./shardbox.pl -s=$s shard $shard obj_space $space index $i drop";
            print  ($command);
            system ($command) unless $self->debug;
        }
    }

}
sub create_shards {
    my $self = shift;
    my $s_per_node = int($self->shard_count / scalar (@{$self->nodes}));
    my $s_residue = $self->shard_count % scalar (@{$self->nodes});
    my $current = 1;
    my $nodes_names = $self->_nodes_names();
    for my $node (@{$self->nodes}) {
        my $count = $s_per_node;
        if ($s_residue) {
            $count++;
            $s_residue--;
        }
        my $save_node_name = delete $nodes_names->{$node};
        for my $shard ($current..$current + $count - 1){
            my $replicas = join ' ', values %$nodes_names;
            print "./shardbox.pl -s=$node shard $shard create por $replicas";
            system("./shardbox.pl -s=$node shard $shard create por $replicas") unless $self->debug;
        }
        $nodes_names->{$node} = $save_node_name;
        $current+= $count;
        print "===================================";
    }
}

sub create_spaces {
    my $self = shift;
    my $s = $self->nodes->[0];
    my $space = $self->space;
    my $unique_index = $self->indexes->[0];
    die "Need unique index" unless $unique_index; 
    for (1..$self->shard_count){
        print "./shardbox.pl -s=$s shard $_ obj_space $space create $unique_index";
        system("./shardbox.pl -s=$s shard $_ obj_space $space create $unique_index") unless $self->debug;
    }
}

sub create_indexes {
    my $self = shift;
    my $s = $self->nodes->[0];
    my $space = $self->space;
    for my $shard (1..$self->shard_count) {
        for ( my $i = 1; $i < @{$self->indexes}; $i++){
            my $command = "./shardbox.pl -s=$s shard $shard obj_space $space index $i create ".$self->indexes->[$i];
            print  ($command);
            system ($command) unless $self->debug;
        }
    }

}

__PACKAGE__->meta->make_immutable();
1;
=begin 
Shard create/alter:
shardbox.pl -s=<HOST> shard <SID> create por [REPLICA1] [REPLICA2] [REPLICA3] [REPLICA4]
shardbox.pl -s=<HOST> shard <SID> create paxos <MASTER2> <MASTER2>
shardbox.pl -s=<HOST> shard <SID> create part <MASTER>

shardbox.pl -s=<HOST> shard <SID> add_replica <NAME>
shardbox.pl -s=<HOST> shard <SID> del_replica <NAME>
shardbox.pl -s=<HOST> shard <SID> master <NAME>
shardbox.pl -s=<HOST> shard <SID> delete
shardbox.pl -s=<HOST> shard <SID> undummy
shardbox.pl -s=<HOST> shard <SID> type <por|paxos|part>

Object space create/drop/truncate:
shardbox.pl -s=<HOST> shard <SID> obj_space <OID> create [no_snap] [no_wal] <INDEX CONF>
shardbox.pl -s=<HOST> shard <SID> obj_space <OID> drop
shardbox.pl -s=<HOST> shard <SID> obj_space <OID> truncate

Index create/drop:
shardbox.pl -s=<HOST> shard <SID> obj_space <OID> index <IID> create <INDEX CONF>
shardbox.pl -s=<HOST> shard <SID> obj_space <OID> index <IID> drop

where:
   HOST: addr:port
   SID: 0-65535
   OID: 0-255
   IID: 0-9
   MASTER, REPLICA1-4: peer.name from octopus.cfg
   INDEX CONF: <INDEX TYPE> [unique] <FIELD0 CONF> [FIELD1 CONF] ... [FIELD7 CONF]
   INDEX TYPE: hash|numhash|tree|sptree|fasttree|compacttree
   FIELD CONF: <FIELD TYPE> <FID> [desc|asc]
   FID: 0-255
   FIELD TYPE: unum16|snum16|unum32|snum32|unum64|snum64|string
=cut
